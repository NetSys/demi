package akka.dispatch.verification

import akka.actor.Cell,
       akka.actor.ActorSystem,
       akka.actor.ActorRef,
       akka.actor.LocalActorRef,
       akka.actor.ActorRefWithCell,
       akka.actor.Actor,
       akka.actor.PoisonPill,
       akka.actor.Props

import akka.dispatch.Envelope,
       akka.dispatch.MessageQueue,
       akka.dispatch.MessageDispatcher

import scala.collection.concurrent.TrieMap,
       scala.collection.mutable.Queue,
       scala.collection.mutable.HashMap,
       scala.collection.mutable.HashSet,
       scala.collection.mutable.ArrayBuffer,
       scala.collection.mutable.ArraySeq,
       scala.collection.mutable.Stack,
       scala.collection.mutable.PriorityQueue,
       scala.math.Ordering,
       scala.reflect.ClassTag

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import Function.tupled

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

// TODO(cs): remove FailureDetector support. Not used.
// TODO(cs): remove Partition/Unpartition support? Not strictly needed.
// TODO(cs): don't assume enqueue_message is just for timers.

object DPORwHeuristics {
  // Tuple is:
  // (depth where race occurs, (racingEvent1, racingEvent2), events to replay
  //  including the racing events)
  type BacktrackKey = (Int, (Unique, Unique), List[Unique])

  // Called whenever we fail to find the violation and start to explore a new
  // trace
  type ResetCallback = () => Unit

  // Called whenever we find a new violation-producing execution
  type ProgressCallback = (EventTrace) => Unit
  var progressCallback : DPORwHeuristics.ProgressCallback = (_) => None
  def setProgressCallback(c: DPORwHeuristics.ProgressCallback) { progressCallback = c }
}

/**
 * DPOR scheduler.
 *   - prioritizePendingUponDivergence: if true, whenever an expected message in
 *     nextTrace does not show up, rather than picking a random pending event
 *     to proceed, instead pick the pending event that appears first in
 *     nextTrace.
 *   - backtrackHeuristic: determines how backtrack points are prioritized.
 *   - If invariant_check_interval is <=0, only checks the invariant at the end of
 *     the execution. Otherwise, checks the invariant every
 *     invariant_check_interval message deliveries.
 *   - depth_bound: determines the maximum depGraph path from the root to the messages
 *     played. Note that this differs from maxMessagesToSchedule.
 */
class DPORwHeuristics(schedulerConfig: SchedulerConfig,
  prioritizePendingUponDivergence:Boolean=false,
  backtrackHeuristic:BacktrackOrdering=new DefaultBacktrackOrdering,
  invariant_check_interval:Int=0,
  depth_bound:Option[Int]=None,
  stopIfViolationFound:Boolean=true,
  startFromBackTrackPoints:Boolean=true,
  skipBacktrackComputation:Boolean=false,
  stopAfterNextTrace:Boolean=false,
  budgetSeconds:Long=Long.MaxValue) extends Scheduler with TestOracle {

  val log = LoggerFactory.getLogger("DPOR")

  def getName: String = "DPORwHeuristics"

  var stopWatch = new Stopwatch(budgetSeconds)

  final val SCHEDULER = "__SCHEDULER__"
  final val PRIORITY = "__PRIORITY__"
  type Trace = Queue[Unique]

  // N.B. this need to be near the top of the class definition.
  private[this] var _root = Unique(MsgEvent("null", "null", null), 0)
  def getRootEvent() : Unique = {
    require(_root != null)
    addGraphNode(_root)
    _root
  }

  var should_bound = false
  var stop_at_depth = 0
  depth_bound match {
    case Some(d) => setDepthBound(d)
    case _ => None
  }
  def setDepthBound(_stop_at_depth: Int) {
    should_bound = true
    stop_at_depth = _stop_at_depth
  }
  var should_cap_messages = false
  var max_messages = 0
  def setMaxMessagesToSchedule(_max_messages: Int) {
    should_cap_messages = true
    max_messages = _max_messages
  }
  var should_cap_distance = false
  var stop_at_distance = 0
  def setMaxDistance(_stop_at_distance: Int) {
    should_cap_distance = true
    stop_at_distance = _stop_at_distance
  }

  // Collection of all actors that could possibly have messages sent to them.
  var actorNameProps : Option[Seq[Tuple2[Props,String]]] = None
  def setActorNameProps(actorNamePropPairs: Seq[Tuple2[Props,String]]) {
    actorNameProps = Some(actorNamePropPairs)
  }

  // When we ignore a message we expected as part of nextTrace, we'll signal to this
  // callback that we just ignored the event at the given index.
  var ignoreAbsentCallback : STSScheduler.IgnoreAbsentCallback = (i: Int) => None
  def setIgnoreAbsentCallback(c: STSScheduler.IgnoreAbsentCallback) {
    ignoreAbsentCallback = c
  }
  var resetCallback : DPORwHeuristics.ResetCallback = () => None
  def setResetCallback(c: DPORwHeuristics.ResetCallback) {
    resetCallback = c
  }

  var instrumenter = Instrumenter
  var externalEventList : Seq[ExternalEvent] = Vector()
  var externalEventIdx = 0
  var started = false

  var currentTime = 0
  var interleavingCounter = 0

  // { (snd,rcv) => pending messages }
  val pendingEvents = new HashMap[(String,String), Queue[(Unique, Cell, Envelope)]]
  val actorNames = new HashSet[String]

  val depGraph = Graph[Unique, DiEdge]()

  val quiescentPeriod = new HashMap[Unique, Int]

  implicit def orderedBacktrackKey(t: DPORwHeuristics.BacktrackKey) = backtrackHeuristic.getOrdered(t)
  val backTrack = new PriorityQueue[DPORwHeuristics.BacktrackKey]()
  var exploredTracker = new ExploredTacker()

  private[this] var currentRoot = getRootEvent

  val currentTrace = new Trace
  val nextTrace = new Trace
  var origNextTraceSize = 0
  // Shortest trace that has ended in an invariant violation.
  var shortestTraceSoFar : Trace = null
  var parentEvent = getRootEvent
  var currentDepth = 0
  // N.B. slightly different in meaning than currentDepth. See
  // setParentEvent().
  var messagesScheduledSoFar = 0
  // When we took the last checkpoint.
  var lastCheckpoint = messagesScheduledSoFar

  // sender -> receivers the sender is currently partitioned from.
  // (Unidirectional)
  val partitionMap = new HashMap[String, HashSet[String]]
  // similar to partitionMap, but only applies to actors that have been
  // created but not yet Start()ed
  val isolatedActors = new HashSet[String]

  var post: (Trace) => Unit = nullFunPost
  var done: (Graph[Unique, DiEdge]) => Unit = nullFunDone

  var currentQuiescentPeriod = 0
  var awaitingQuiescence = false
  var nextQuiescentPeriod = 0
  var quiescentMarker:Unique = null

  var test_invariant : Invariant = schedulerConfig.invariant_check match {
    case Some(i) => i
    case None => null
  }

  // Whether we are currently awaiting checkpoint responses from actors.
  var blockedOnCheckpoint = new AtomicBoolean(false)
  var stats: MinimizationStats = null
  var currentSubsequence : Seq[ExternalEvent] = null
  var lookingFor : ViolationFingerprint = null
  var foundLookingFor = false
  var _initialTrace : Trace = null
  def setInitialTrace(t: Trace) {
    _initialTrace = t
  }
  var _initialDepGraph : Graph[Unique, DiEdge] = null
  def setInitialDepGraph(g: Graph[Unique, DiEdge]) {
    _initialDepGraph = g
  }
  var checkpointer : CheckpointCollector =
    if (schedulerConfig.enableCheckpointing) new CheckpointCollector else null

  def nullFunPost(trace: Trace) : Unit = {}
  def nullFunDone(graph: Graph[Unique, DiEdge]) : Unit = {}

  // Reset state between runs
  private[this] def reset() = {
    /**
     * We leave these as they are:
    interleavingCounter = 0
    exploredTracker.clear
    depGraph.clear
    backTrack.clear
    stopWatch
     */
    pendingEvents.clear()
    currentDepth = 0
    currentTrace.clear
    nextTrace.clear
    origNextTraceSize = 0
    partitionMap.clear
    awaitingQuiescence = false
    nextQuiescentPeriod = 0
    quiescentMarker = null
    currentQuiescentPeriod = 0
    externalEventIdx = 0
    currentTime = 0
    actorNames.clear
    quiescentPeriod.clear
    blockedOnCheckpoint.set(false)
    foundLookingFor = false
    messagesScheduledSoFar = 0
    lastCheckpoint = 0
    currentRoot = getRootEvent

    setParentEvent(getRootEvent)
  }

  private[this] def awaitQuiescenceUpdate (nextEvent:Unique) = {
    log.trace(Console.BLUE + "Beginning to wait for quiescence " + Console.RESET)
    nextEvent match {
      case Unique(WaitQuiescence(), id) =>
        awaitingQuiescence = true
        nextQuiescentPeriod = id
        quiescentMarker = nextEvent
      case _ =>
        throw new Exception("Bad args")
    }
  }

  private[this] def getPathLength(event: Unique) : Int = {
    depGraph.synchronized {
      val graphNode = depGraph.get(event)
      val rootNode = depGraph.get(getRootEvent)
      return graphNode.pathTo(rootNode) match {
        case Some(p) => p.length
        case _ =>
          throw new Exception("Unexpected path")
      }
    }
  }

  private[this] def setParentEvent (event: Unique) {
    val pathLength = getPathLength(event)
    parentEvent = event
    currentDepth = pathLength + 1
  }

  private[this] def addGraphNode (event: Unique) = {
    require(event != null)
    depGraph.synchronized {
      depGraph.add(event)
      quiescentPeriod(event) = currentQuiescentPeriod
    }
  }

  // Before adding a new WaitQuiescence/NetworkPartition/NetworkUnpartition to depGraph,
  // first try to see if it already exists in depGraph.
  def maybeAddGraphNode(unique: Unique): Unique = {
    depGraph.synchronized {
      depGraph.get(currentRoot).inNeighbors.find {
        x => x.value match {
          case Unique(WaitQuiescence(), id) =>
            log.debug("Existing WaitQuiescence: " + id + ". looking for: " + unique.id)
            id == unique.id
          case _ => false
        }
      } match {
        case Some(e) =>
          return e.value
        case None =>
          addGraphNode(unique)
          depGraph.addEdge(unique, currentRoot)(DiEdge)
          return unique
      }
    }
  }

  def decomposePartitionEvent(event: NetworkPartition) : Queue[(String, NodesUnreachable)] = {
    val queue = new Queue[(String, NodesUnreachable)]
    queue ++= event.first.map { x => (x, NodesUnreachable(event.second)) }
    queue ++= event.second.map { x => (x, NodesUnreachable(event.first)) }
    return queue
  }

  def decomposeUnPartitionEvent(event: NetworkUnpartition) : Queue[(String, NodesReachable)] = {
    val queue = new Queue[(String, NodesReachable)]
    queue ++= event.first.map { x => (x, NodesReachable(event.second)) }
    queue ++= event.second.map { x => (x, NodesReachable(event.first)) }
    return queue
  }

  def isSystemCommunication(sender: ActorRef, receiver: ActorRef) : Boolean =
    throw new Exception("not implemented")

  override def isSystemCommunication(sender: ActorRef, receiver: ActorRef, msg: Any): Boolean =
  (receiver, sender) match {
    case (null, _) => return true
    case (_, null) => isSystemMessage("deadletters", receiver.path.name, msg)
    case _ => isSystemMessage(sender.path.name, receiver.path.name, msg)
  }

  // Is this message a system message
  def isValidActor(sender: String, receiver: String): Boolean =
  ((actorNames contains sender) || (actorNames contains receiver)) match {
    case true => return true
    case _ => return false
  }

  def isSystemMessage(sender: String, receiver: String) : Boolean =
    throw new Exception("not implemented")

  // Is this message a system message
  override def isSystemMessage(sender: String, receiver: String, msg: Any): Boolean = {
    return !isValidActor(sender, receiver) || receiver == "deadLetters"
  }

  // Notification that the system has been reset
  def start_trace() : Unit = {
    resetCallback()
    started = false
    actorNames.clear
    externalEventIdx = 0
    setParentEvent(getRootEvent)

    currentTrace += getRootEvent
    if (stats != null) {
      stats.increment_replays()
    }
    maybeStartActors()
    runExternal()
  }

  // When executing a trace, find the next trace event.
  def mutableTraceIterator(trace: Trace) : Option[Unique] =
  trace.isEmpty match {
    case true => return None
    case _ => return Some(trace.dequeue)
  }

  // Get next message event from the trace.
  def getNextTraceMessage() : Option[Unique] =
  mutableTraceIterator(nextTrace) match {
    // All spawn events are ignored.
    case some @ Some(Unique(s: SpawnEvent, id)) => getNextTraceMessage()
    // All system messages need to ignored.
    case some @ Some(Unique(t, 0)) => getNextTraceMessage()
    case some @ Some(Unique(t, id)) => some
    case None => None
    case _ => throw new Exception("internal error")
  }

  // TODO(cs): could potentially optimize this a bit. If a node hasn't
  // received a message since the last checkpoint, no need to send it a
  // CheckpointRequest.
  def prepareCheckpoint() {
    log.debug("Initiating checkpoint..")
    val actorRefs = Instrumenter().actorMappings.
                      filterNot({case (k,v) => ActorTypes.systemActor(k)}).
                      values.toSeq
    val checkpointRequests = checkpointer.prepareRequests(actorRefs)
    // Put our requests at the front of the queue, and any existing requests
    // at the end of the queue.
    Instrumenter().sendKnownExternalMessages(() => {
      for ((actor, request) <- checkpointRequests) {
        Instrumenter().actorMappings(actor) ! request
      }
    })
    Instrumenter().await_enqueue
  }

  // Global invariant.
  def checkInvariant() {
    log.info("Checking invariant")
    val checkpoint = if (checkpointer != null) checkpointer.checkpoints
                      else new HashMap[String, Option[CheckpointReply]]
    val violation = test_invariant(currentSubsequence, checkpoint)
    violation match {
      case Some(v) =>
        if (lookingFor.matches(v)) {
          log.info("Found matching violation!")
          DPORwHeuristics.progressCallback(
            DPORwHeuristicsUtil.convertToEventTrace(currentTrace,
              currentSubsequence))
          foundLookingFor = true
          if (shortestTraceSoFar == null ||
              currentTrace.length < shortestTraceSoFar.length) {
            shortestTraceSoFar = currentTrace.clone()
          }
          return
        } else {
          log.info("No matching violation. Proceeding...")
        }
      case None =>
        log.info("No matching violation. Proceeding...")
    }
  }

  // Figure out what is the next message to schedule.
  def schedule_new_message(blockedActors: Set[String]) : Option[(Cell, Envelope)] = {
    // Stop if we found the original invariant violation.
    if (foundLookingFor) {
      return None
    }

    // Find equivalent messages to the one we are currently looking for.
    def equivalentTo(u1: Unique, other: (Unique, Cell, Envelope)) :
    Boolean = (u1, other._1) match {
      case (Unique(MsgEvent(_, rcv1, _), id1),
            Unique(MsgEvent(_, rcv2, _), id2) ) =>
        // If the ID is zero, this means it's a system message.
        // In that case compare only the receivers.
        if (id1 == 0) rcv1 == rcv2
        else rcv1 == rcv2 && id1 == id2
      case (Unique(_, id1), Unique(_, id2) ) => id1 == id2
      case _ => false
    }

    // Get from the current set of pending events.
    def getPendingEvent(): Option[(Unique, Cell, Envelope)] = {
      // Do we have some pending events
      pendingEvents.find({
        case (k,v) => !(blockedActors contains k._2) && !v.isEmpty
      }).map(o => o._2.head) match {
        case Some( next @ (Unique(MsgEvent(snd, rcv, msg), id), _, _)) =>
          log.trace( Console.GREEN + "Now playing pending: "
              + "(" + snd + " -> " + rcv + ") " +  + id + Console.RESET )
          pendingEvents((snd,rcv)).dequeueFirst(e => e == next)
          Some(next)

        case Some(par @ (Unique(NetworkPartition(part1, part2), id), _, _)) =>
          log.trace( Console.GREEN + "Now playing the high level partition event " +
              id + Console.RESET)
          pendingEvents((SCHEDULER,SCHEDULER)).dequeueFirst(e => e == par)
          Some(par)

        case Some(par @ (Unique(NetworkUnpartition(part1, part2), id), _, _)) =>
          log.trace( Console.GREEN + "Now playing the high level partition event " +
              id + Console.RESET)
          pendingEvents((SCHEDULER,SCHEDULER)).dequeueFirst(e => e == par)
          Some(par)

        case Some(qui @ (Unique(WaitQuiescence(), id), _, _)) =>
          log.trace( Console.GREEN + "Now playing the high level quiescence event " +
              id + Console.RESET)
          pendingEvents((SCHEDULER,SCHEDULER)).dequeueFirst(e => e == qui)
          Some(qui)

        case None => None
        case _ => throw new Exception("internal error")
      }
    }

    def getMatchingMessage() : Option[(Unique, Cell, Envelope)] = {
      getNextTraceMessage() match {
        // The trace says there is a message event to run.
        case Some(u @ Unique(MsgEvent(snd, rcv, WildCardMatch(msgSelector,_)), id)) =>
          if (blockedActors contains rcv)
            None
          else
            pendingEvents.get((snd,rcv)) match {
              case Some(queue) =>
                // queue should already be ordered by send order
                val pending = queue.toIndexedSeq
                def backtrackSetter(idx: Int) {
                  val priorEvent = currentTrace.last
                  val toPlayNext = pending(idx)._1
                  val commonPrefix = getCommonPrefix(priorEvent, toPlayNext)
                  val lastElement = commonPrefix.last
                  val branchI = currentTrace.indexWhere { e => (e == lastElement.value) }
                  log.trace(s"Setting backtrack@${branchI} ${priorEvent} ${toPlayNext}")
                  // TODO(cs): too specific to FungibleClockClustering.. we abuse
                  // the last tuple item to include our remaining WildCards [nextTrace.clone]
                  // N.B. currentTrace's head is the root event, so we skip over it.
                  val traceToPlayNext = currentTrace.tail.clone.toList ++ List(toPlayNext) ++ nextTrace.clone.toList
                  backTrack.enqueue((branchI, (priorEvent, toPlayNext), traceToPlayNext))
                }
                val pendingMessages = pending.map { case t => t._3.message }
                msgSelector(pendingMessages, backtrackSetter) match {
                  case Some(idx) =>
                    val found = pending(idx)
                    // Mark this as done ordering done. // TODO(cs): possibly
                    // not necessary
                    val priorEvent = currentTrace.last
                    val branchI = currentTrace.length - 1
                    exploredTracker.setExplored(branchI, (priorEvent, found._1))
                    val msgEvent = found._1.event
                    queue.dequeueFirst(e => e == found)
                  case None => None
                }
              case None =>  None
            }

        case Some(u @ Unique(MsgEvent(snd, rcv, msg), id)) =>
          // Look at the pending events to see if such message event exists.
          if (blockedActors contains rcv)
            None
          else
            pendingEvents.get((snd,rcv)) match {
              case Some(queue) => queue.dequeueFirst(equivalentTo(u, _))
              case None =>  None
            }

        case Some(u @ Unique(NetworkPartition(_, _), id)) =>
          // Look at the pending events to see if such message event exists.
          pendingEvents.get((SCHEDULER,SCHEDULER)) match {
            case Some(queue) => queue.dequeueFirst(equivalentTo(u, _))
            case None =>  None
          }

        case Some(u @ Unique(NetworkUnpartition(_, _), id)) =>
          // Look at the pending events to see if such message event exists.
          pendingEvents.get((SCHEDULER,SCHEDULER)) match {
            case Some(queue) => queue.dequeueFirst(equivalentTo(u, _))
            case None =>  None
          }

        case Some(u @ Unique(WaitQuiescence(), _)) => // Look at the pending events to see if such message event exists.
          pendingEvents.get((SCHEDULER,SCHEDULER)) match {
            case Some(queue) => queue.dequeueFirst(equivalentTo(u, _))
            case None =>  None
          }

        // The trace says there is nothing to run so we have either exhausted our
        // trace or are running for the first time. Use any enabled transitions.
        case None => None
        case _ => throw new Exception("internal error")
      }
    }
 
    // Keep looping until we find a pending message that matches.
    // Note that this mutates nextTrace; it pops from the head of the queue
    // until it finds a match or until the queue is empty.
    def getNextMatchingMessage(): Option[(Unique, Cell, Envelope)] = {
      var foundMatching : Option[(Unique, Cell, Envelope)] = None
      while (!nextTrace.isEmpty && foundMatching == None) {
        val expecting = nextTrace.head
        // idx from the original nextTrace
        val idx = origNextTraceSize - nextTrace.size
        foundMatching = getMatchingMessage()
        if (foundMatching.isEmpty) {
          log.debug("Ignoring " + expecting)
          ignoreAbsentCallback(idx)
        }
      }
      return foundMatching
    }

    // Now on to actually picking a message to schedule.
    // First check if it's time to check invariants.
    if (schedulerConfig.enableCheckpointing &&
        !blockedOnCheckpoint.get &&
        invariant_check_interval > 0 &&
        (messagesScheduledSoFar % invariant_check_interval) == 0 &&
        lastCheckpoint != messagesScheduledSoFar) {
      lastCheckpoint = messagesScheduledSoFar
      if (checkpointer == null) {
        // If checkpointing not enabled, test the invariant now
        checkInvariant()
      } else {
        prepareCheckpoint()
      }
    }

    // Are there any prioritized events that need to be dispatched.
    pendingEvents.get((PRIORITY,PRIORITY)) match {
      case Some(queue) if !queue.isEmpty => {
        val (_, cell, env) = queue.dequeue()
        return Some((cell, env))
      }
      case _ => None
    }

    // Actual (non-priority) messages scheduled so far.
    messagesScheduledSoFar += 1
    if (should_cap_messages && messagesScheduledSoFar > max_messages) {
      return None
    }

    if (stopAfterNextTrace && nextTrace.isEmpty) {
      return None
    }

    val result = awaitingQuiescence match {
      case false =>
        ((prioritizePendingUponDivergence match {
          case true => getNextMatchingMessage()
          case false => getMatchingMessage()
        }): @unchecked) match {
          // There is a pending event that matches a message in our trace.
          // We call this a convergent state.
          case m @ Some((u @ Unique(MsgEvent(snd, rcv, msg), id), cell, env)) =>
            log.trace( Console.GREEN + "Replaying the exact message: Message: " +
                "(" + snd + " -> " + rcv + ") " +  + id + Console.RESET )
            Some((u, cell, env))

          case Some((u @ Unique(NetworkPartition(part1, part2), id), _, _)) =>
            log.trace( Console.GREEN + "Replaying the exact message: Partition: ("
                + part1 + " <-> " + part2 + ")" + Console.RESET )
            Some((u, null, null))

          case Some((u @ Unique(NetworkUnpartition(part1, part2), id), _, _)) =>
            log.trace( Console.GREEN + "Replaying the exact message: Unpartition: ("
                + part1 + " <-> " + part2 + ")" + Console.RESET )
            Some((u, null, null))

          case Some((u @ Unique(WaitQuiescence(), id), _, _)) =>
            log.trace( Console.GREEN + "Replaying the exact message: Quiescence: ("
                + id +  ")" + Console.RESET )
            Some((u, null, null))

          // We call this a divergent state.
          case None =>
            if (stopAfterNextTrace && nextTrace.isEmpty) {
              None
            } else {
              getPendingEvent()
            }
        }
      case true => // Don't call getMatchingMessage when waiting quiescence. Except when divergent or running the first
                  // time through, there should be no pending messages, signifying quiescence. Get pending event takes
                  // care of the first run. We could explicitly detect divergence here, but we haven't been so far.
        getPendingEvent()
    }

    result match {
      case Some((nextEvent @ Unique(MsgEvent(snd, rcv, msg), nID), cell, env)) =>
        if ((isolatedActors contains snd) || (isolatedActors contains rcv)) {
          log.trace(Console.RED + "Discarding event " + nextEvent +
            " due to not yet started node " + snd + " or " + rcv + Console.RESET)
          // Self-messages without any prior messages break our assumptions.
          // Fix might be to store this message intead of dropping it.
          if (snd == rcv) {
            throw new RuntimeException("self message without prior messages! " + snd)
          }
          return schedule_new_message(blockedActors)
        }
        partitionMap.get(snd) match {
          case Some(set) =>
            if (set.contains(rcv)) {
              log.trace(Console.RED + "Discarding event " + nextEvent +
                " due to partition" + Console.RESET)
              return schedule_new_message(blockedActors)
            }
          case _ =>
        }
        currentTrace += nextEvent
        setParentEvent(nextEvent)

        return Some((cell, env))

      case Some((nextEvent @ Unique(par@ NetworkPartition(first, second), nID), _, _)) =>
        // FIXME: We cannot send NodesUnreachable messages to FSM: in response to an unexpected message,
        // an FSM cancels and then restart a timer. This means that we cannot actually make this assumption
        // below
        //
        // A NetworkPartition event is translated into multiple
        // NodesUnreachable messages which are atomically and
        // and invisibly consumed by all relevant parties.
        // Important: no messages are allowed to be dispatched
        // as the result of NodesUnreachable being received.
        //decomposePartitionEvent(par) map tupled(
          //(rcv, msg) => instrumenter().actorMappings(rcv) ! msg)
        for (node  <- first) {
          partitionMap(node) = partitionMap.getOrElse(node, new HashSet[String]) ++  second
        }
        for (node  <- second) {
          partitionMap(node) = partitionMap.getOrElse(node, new HashSet[String]) ++  first
        }

        instrumenter().tellEnqueue.await()

        currentTrace += nextEvent
        return schedule_new_message(blockedActors)

      case Some((nextEvent @ Unique(par@ NetworkUnpartition(first, second), nID), _, _)) =>
        // FIXME: We cannot send NodesUnreachable messages to FSM: in response to an unexpected message,
        // an FSM cancels and then restart a timer. This means that we cannot actually make this assumption
        // below
        //decomposeUnPartitionEvent(par) map tupled(
          //(rcv, msg) => instrumenter().actorMappings(rcv) ! msg)

        for (node  <- first) {
          partitionMap.get(node) match {
            case Some(set) => set --= second
            case _ =>
          }
        }
        for (node  <- second) {
          partitionMap.get(node) match {
            case Some(set) => set --= first
            case _ =>
          }
        }

        instrumenter().tellEnqueue.await()

        currentTrace += nextEvent
        return schedule_new_message(blockedActors)

      case Some((nextEvent @ Unique(WaitQuiescence(), nID), _, _)) =>
        awaitQuiescenceUpdate(nextEvent)
        return schedule_new_message(blockedActors)

      case _ =>
        return None
    }
  }

  def next_event() : Event = {
    throw new Exception("not implemented next_event()")
  }

  // Record that an event was consumed
  def event_consumed(event: Event) = {}

  def event_consumed(cell: Cell, envelope: Envelope) = {}

  def event_produced(event: Event) = event match {
    case event : SpawnEvent => actorNames += event.name
    case msg : MsgEvent =>
  }

  // Ensure that all possible actors are created, not just those with Start
  // events. This is to prevent issues with tellEnqueue getting confused.
  def maybeStartActors() {
    // Just start and isolate all actors we might eventually care about
    actorNameProps match {
      case Some(seq) =>
        for ((props, name) <- seq) {
          Instrumenter().actorSystem.actorOf(props, name)
        }
      case None =>
        None
    }

    isolatedActors ++= actorNames

    if (schedulerConfig.enableCheckpointing) {
      checkpointer.startCheckpointCollector(Instrumenter().actorSystem)
    }
  }

  def runExternal() = {
    log.trace(Console.RED + " RUN EXTERNAL CALLED initial IDX = " + externalEventIdx +Console.RESET)

    def processExternal() {
      var await = false
      while (externalEventIdx < externalEventList.length && !await) {
        val event = externalEventList(externalEventIdx)
        event match {
          case Start(propsCtor, name) =>
            // If not already started: start it and unisolate it
            if (!(instrumenter().actorMappings contains name)) {
              instrumenter().actorSystem().actorOf(propsCtor(), name)
            }
            isolatedActors -= name

          case Send(rcv, msgCtor) =>
            val ref = instrumenter().actorMappings(rcv)
            log.trace(Console.BLUE + " sending " + rcv + " messge " + msgCtor() + Console.RESET)
            instrumenter().actorMappings(rcv) ! msgCtor()

          case uniq @ Unique(par : NetworkPartition, id) =>
            val msgs = pendingEvents.getOrElse((SCHEDULER,SCHEDULER), new Queue[(Unique, Cell, Envelope)])
            pendingEvents((SCHEDULER,SCHEDULER)) = msgs += ((uniq, null, null))
            maybeAddGraphNode(uniq)

          case uniq @ Unique(par : NetworkUnpartition, id) =>
            val msgs = pendingEvents.getOrElse((SCHEDULER,SCHEDULER), new Queue[(Unique, Cell, Envelope)])
            pendingEvents((SCHEDULER,SCHEDULER)) = msgs += ((uniq, null, null))
            maybeAddGraphNode(uniq)

          case event @ Unique(WaitQuiescence(), _) =>
            val msgs = pendingEvents.getOrElse((SCHEDULER,SCHEDULER), new Queue[(Unique, Cell, Envelope)])
            pendingEvents((SCHEDULER,SCHEDULER)) = msgs += ((event, null, null))
            maybeAddGraphNode(event)
            await = true

          // A unique ID needs to be associated with all network events.
          case par : NetworkPartition => throw new Exception("internal error")
          case par : NetworkUnpartition => throw new Exception("internal error")
          case _ => throw new Exception("unsuported external event " + event)
        }
        externalEventIdx += 1
      }

      log.trace(Console.RED + " RUN EXTERNAL LOOP ENDED idx = " + externalEventIdx + Console.RESET)
    }

    instrumenter().sendKnownExternalMessages(processExternal)
    instrumenter().tellEnqueue.await()
    instrumenter().start_dispatch
  }

  def run(externalEvents: Seq[ExternalEvent],
          f1: (Trace) => Unit = nullFunPost,
          f2: (Graph[Unique, DiEdge]) => Unit = nullFunDone,
          initialTrace: Option[Trace] = None,
          initialGraph: Option[Graph[Unique, DiEdge]] = None) = {
    reset()
    // Transform the original list of external events,
    // and assign a unique ID to all network events.
    // This is necessary since network events are not
    // part of the dependency graph.
    externalEventList = externalEvents.map { e => e match {
      case par: NetworkPartition =>
        val unique = Unique(par)
        unique
      case par: NetworkUnpartition =>
        val unique = Unique(par)
        unique
      case w: WaitQuiescence =>
        Unique(w, id=w._id)
      case Unique(start : Start, _) =>
        start
      case Unique(send : Send, _) =>
        send
      case other => other
    } }

    post = f1
    done = f2

    initialTrace match {
      case Some(tr) =>
        nextTrace.clear()
        nextTrace ++= tr
        origNextTraceSize = nextTrace.size
        initialGraph match {
          case Some(gr) =>
            depGraph.synchronized {
              depGraph ++= gr
            }
          case _ => throw new Exception("Must supply a dependency graph with a trace")
        }
      case _ =>
        nextTrace.clear
        origNextTraceSize = 0
    }

    // In the end, reinitialize_system call start_trace.
    instrumenter().reinitialize_system(null, null)
  }

  /**
   * Given a message, figure out if we have already seen
   * it before. We achieve this by consulting the
   * dependency graph.
   *
   * * @param (cell, envelope): Original message context.
   *
   * * @return A unique event.
   */
  def getMessage(cell: Cell, envelope: Envelope) : Unique = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgEvent = new MsgEvent(snd, rcv, envelope.message)
    val fingerprinted = schedulerConfig.messageFingerprinter.fingerprint(envelope.message)

    // Who cares if the parentEvent is in fact a message, as long as it is a parent.
    val parent = parentEvent

    def matchMessage (event: Event) : Boolean = {
      event match {
        case MsgEvent(s,r,m) => (s == snd && r == rcv &&
          schedulerConfig.messageFingerprinter.fingerprint(m) == fingerprinted)
        case e => throw new IllegalStateException("Not a MsgEvent: " + e)
      }
    }
    depGraph.synchronized {
      val parentNode = try {
          depGraph.get(parent)
      } catch {
        case e: java.util.NoSuchElementException =>
          throw new IllegalStateException(s"No such parent ${parent}")
      }
      val inNeighs = parentNode.inNeighbors
      inNeighs.find { x => matchMessage(x.value.event) } match {
        case Some(x) => return x.value
        case None =>
          val newMsg = Unique(msgEvent)
          log.trace(
              Console.YELLOW + "Not seen: " + newMsg.id +
              " (" + msgEvent.sender + " -> " + msgEvent.receiver + ") " + msgEvent.msg + Console.RESET)
          return newMsg
        case _ => throw new Exception("wrong type")
      }
    }
  }

  def event_produced(cell: Cell, envelope: Envelope) : Unit = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    if (rcv == CheckpointSink.name) {
      assert(schedulerConfig.enableCheckpointing)
      checkpointer.handleCheckpointResponse(envelope.message, snd)
      if (checkpointer.done && !blockedOnCheckpoint.get) {
        checkInvariant()
      }

      // Don't add anything to pendingEvents, since we just "delivered" it
      return
    }

    envelope.message match {
      // CheckpointRequests and failure detector messeages are simply enqueued to the priority queued
      // and dispatched at the earliest convenience.
      case NodesReachable | NodesUnreachable | CheckpointRequest =>
        val msgs = pendingEvents.getOrElse((PRIORITY,PRIORITY), new Queue[(Unique, Cell, Envelope)])
        pendingEvents((PRIORITY,PRIORITY)) = msgs += ((null, cell, envelope))

      case _ =>
        val unique @ Unique(msg : MsgEvent, id) = getMessage(cell, envelope)
        val msgs = pendingEvents.getOrElse((msg.sender,msg.receiver), new Queue[(Unique, Cell, Envelope)])
        // Do not enqueue if bound hit
        if (!should_bound || currentDepth < stop_at_depth) {
          log.trace(Console.BLUE + "Enqueuing event " + cell + " " + envelope + " " + unique +
                       " (" + parentEvent + ") " + Console.RESET)
          pendingEvents((msg.sender,msg.receiver)) = msgs += ((unique, cell, envelope))
        } else {
          log.debug(Console.RED + "Not adding message because we hit depth bound " + Console.RESET)
        }

        log.debug(Console.BLUE + "New event: " +
            "(" + msg.sender + " -> " + msg.receiver + ") " +
            id + Console.RESET)

        depGraph.synchronized {
          addGraphNode(unique)
          depGraph.addEdge(unique, parentEvent)(DiEdge)
        }
    }
  }

  // Called before we start processing a newly received event
  def before_receive(cell: Cell) {}

  // Called after receive is done being processed
  def after_receive(cell: Cell) {}

  def notify_quiescence() {
    if (foundLookingFor) {
      // We're done.
      done(depGraph)
      return
    }

    if (awaitingQuiescence) {
      awaitingQuiescence = false
      log.trace(Console.BLUE + " Done waiting for quiescence " + Console.RESET)

      currentQuiescentPeriod = nextQuiescentPeriod
      nextQuiescentPeriod = 0

      val marker = maybeAddGraphNode(quiescentMarker)
      currentTrace += marker
      currentRoot = marker
      setParentEvent(marker)
      quiescentMarker = null

      runExternal()
    } else {
      if (test_invariant != null) {
        if (blockedOnCheckpoint.get) {
          // We've finished our checkpoint. Check our invariant. If it fails,
          // stop exploring! Otherwise, continue exploring schedules.
          blockedOnCheckpoint.set(false)
          checkInvariant()
          if (foundLookingFor) {
            done(depGraph)
            return
          }
          // fall through to next interleaving
        } else if (checkpointer != null) {
          // Initiate a checkpoint
          blockedOnCheckpoint.set(true)
          prepareCheckpoint()
          Instrumenter().start_dispatch
          return
        } else {
          // Checkpointing is not enabled, so just test it now
          checkInvariant()
          if (foundLookingFor) {
            done(depGraph)
            return
          }
        }
      }

      log.info("\n--------------------- Interleaving #" +
                  interleavingCounter + " ---------------------")

      log.debug(Console.BLUE + "Current trace: " +
          Util.traceStr(currentTrace) + Console.RESET)

      for (Unique(ev, id) <- currentTrace)
        log.debug(Console.BLUE + " " + id + " " + ev + Console.RESET)

      messagesScheduledSoFar = 0
      nextTrace.clear()
      origNextTraceSize = 0

      // Unconditionally post the current trace
      post(currentTrace)

      dpor(currentTrace) match {
        case Some(trace) =>
          nextTrace.clear()
          nextTrace ++= trace
          origNextTraceSize = nextTrace.size

          log.debug(Console.BLUE + "Next trace:  " +
              Util.traceStr(nextTrace) + Console.RESET)

          setParentEvent(getRootEvent)
          currentRoot = getRootEvent

          pendingEvents.clear()
          currentTrace.clear
          currentQuiescentPeriod = 0

          instrumenter().await_enqueue()
          instrumenter().restart_system()
        case None =>
          return
      }
    }
  }

  // Assume: only used for timers (which implies that it's always an
  // akka-dispatcher thread calling this method).
  def enqueue_message(senderOpt: Option[ActorRef], receiver: String,msg: Any): Unit = {
    log.trace(Console.BLUE + "Enqueuing timer to " + receiver + " with msg " + msg + Console.RESET)
    senderOpt match {
      case Some(sendRef) =>
        instrumenter().actorMappings(receiver).!(msg)(sendRef)
      case None =>
        instrumenter().actorMappings(receiver) ! msg
    }
    instrumenter().await_enqueue()
  }

  def shutdown(): Unit = {
    throw new Exception("shutdown not supported for DPOR?")
  }

  def notify_timer_cancel (receiver: String, msg: Any) = {
    log.trace(Console.BLUE + " Trying to cancel timer for " + receiver + " " + msg + Console.BLUE)
    def equivalentTo(u: (Unique, Cell, Envelope)): Boolean = {
      u._3 match {
        case null => false
        case e => e.message == msg && u._2.self.path.name == receiver
      }
    }

    pendingEvents.get(("deadLetters",receiver)) match {
      case Some(q) =>
        q.dequeueFirst(equivalentTo(_)) match {
          case Some(u) => log.trace(Console.RED + " Removing pending event (" +
                    receiver + " , " + msg + ")" + Console.RESET)
          case None => log.trace(Console.RED +
                    " Did not remove message due to timer cancellation (" +
                      receiver + ", " + msg + ")" +  Console.RESET)
        }
      case None => // This cancellation came too late, things have already been done.
        log.trace(Console.RED +
                    " Did not remove message due to timer cancellation (" +
                      receiver + ", " + msg + ")" +  Console.RESET)

    }
  }

  def getEvent(index: Integer, trace: Trace) : Unique = {
    trace(index) match {
      case u: Unique => u
      case _ => throw new Exception("internal error not a message")
    }
  }

  private[dispatch] def getCommonPrefix(earlier: Unique,
                                        later: Unique) : Seq[DPORwHeuristics.this.depGraph.NodeT]= {
    depGraph.synchronized {
      val rootN = ( depGraph get getRootEvent )
      // Get the actual nodes in the dependency graph that
      // correspond to those events
      val earlierN = (depGraph get earlier)
      val laterN = (depGraph get later)

      // Get the dependency path between later event and the
      // root event (root node) in the system.
      val laterPath = laterN.pathTo(rootN) match {
        case Some(path) => path.nodes.toList.reverse
        case None => throw new Exception("no such path")
      }

      // Get the dependency path between earlier event and the
      // root event (root node) in the system.
      val earlierPath = earlierN.pathTo(rootN) match {
        case Some(path) => path.nodes.toList.reverse
        case None => throw new Exception("no such path")
      }

      // Find the common prefix for the above paths.
      return laterPath.intersect(earlierPath)
    }
  }

  def dpor(trace: Trace) : Option[Trace] = {
    if (!stopWatch.anyTimeLeft) {
      log.warn("No time left on stopWatch")
      done(depGraph)
      return None
    }

    depGraph.synchronized {
      interleavingCounter += 1
      val root = getEvent(0, currentTrace)
      val rootN = ( depGraph get getRootEvent )

      val racingIndices = new HashSet[Integer]

      /**
       *  Analyze the dependency between two events that are co-enabled
       ** and have the same receiver.
       *
       ** @param earleirI: Index of the earlier event.
       ** @param laterI: Index of the later event.
       ** @param trace: The trace to which the events belong to.
       *
       ** @return none
       */
      def analyze_dep(earlierI: Int, laterI: Int, trace: Trace):
      Option[(Int, List[Unique])] = {
        // Retrieve the actual events.
        val earlier = getEvent(earlierI, trace)
        val later = getEvent(laterI, trace)

        (earlier.event, later.event) match {
          // Since the later event is completely independent, we
          // can simply move it in front of the earlier event.
          // This might cause the earlier event to become disabled,
          // but we have no way of knowing.
          case (_: MsgEvent, _: NetworkPartition) =>
            val branchI = earlierI - 1
            val needToReplay = List(later, earlier)

            exploredTracker.setExplored(branchI, (earlier, later))
            return Some((branchI, needToReplay))

          // Similarly, we move an earlier independent event
          // just after the later event. None of the two event
          // will become disabled in this case.
          case (_: NetworkPartition, _: MsgEvent) =>
            val branchI = earlierI - 1
            val needToReplay = currentTrace.clone()
              .drop(earlierI + 1)
              .take(laterI - earlierI)
              .toList :+ earlier

            exploredTracker.setExplored(branchI, (earlier, later))
            return Some((branchI, needToReplay))

          // Since the later event is completely independent, we
          // can simply move it in front of the earlier event.
          // This might cause the earlier event to become disabled,
          // but we have no way of knowing.
          case (_: MsgEvent, _: NetworkUnpartition) =>
            val branchI = earlierI - 1
            val needToReplay = List(later, earlier)

            exploredTracker.setExplored(branchI, (earlier, later))
            return Some((branchI, needToReplay))

          // Similarly, we move an earlier independent event
          // just after the later event. None of the two event
          // will become disabled in this case.
          case (_: NetworkUnpartition, _: MsgEvent) =>
            val branchI = earlierI - 1
            val needToReplay = currentTrace.clone()
              .drop(earlierI + 1)
              .take(laterI - earlierI)
              .toList :+ earlier

            exploredTracker.setExplored(branchI, (earlier, later))
            return Some((branchI, needToReplay))

          case (_: MsgEvent, _: MsgEvent) =>
            val commonPrefix = getCommonPrefix(earlier, later)

            // Figure out where in the provided trace this needs to be
            // replayed. In other words, get the last element of the
            // common prefix and figure out which index in the trace
            // it corresponds to.
            val lastElement = commonPrefix.last
            val branchI = trace.indexWhere { e => (e == lastElement.value) }

            val needToReplay = currentTrace.clone()
              .drop(branchI + 1)
              .dropRight(currentTrace.size - laterI - 1)
              .filter { x => x.id != earlier.id }

            require(branchI < laterI)

            // Since we're dealing with the vertices and not the
            // events, we need to extract the values.
            val needToReplayV = needToReplay.toList

            exploredTracker.setExplored(branchI, (earlier, later))
            return Some((branchI, needToReplayV))
        }
        return None
      }

      /** Figure out if two events are co-enabled.
       *
       * See if there is a path from the later event to the
       * earlier event on the dependency graph. If such
       * path does exist, this means that one event disables
       * the other one.
       *
       ** @param earlier: First event
       ** @param later: Second event
       *
       ** @return: Boolean
       */
      def isCoEnabeled(earlier: Unique, later: Unique) : Boolean = (earlier, later) match {
        // Quiescence is never co-enabled
        case (Unique(WaitQuiescence(), _), _) => false
        case (_, Unique(WaitQuiescence(), _)) => false
        // Network partitions and unpartitions with interesection are not coenabled (cheap hack to
        // prevent us from trying reordering these directly). We need to do something smarter maybe
        case (Unique(p1: NetworkPartition, _), Unique(p2: NetworkPartition, _)) => false
        case (Unique(u1: NetworkUnpartition, _), Unique(u2: NetworkUnpartition, _)) => false
        case (Unique(p1: NetworkPartition, _), Unique(u2: NetworkUnpartition, _)) => false
        case (Unique(u1: NetworkUnpartition, _), Unique(p2: NetworkPartition, _)) => false
        // NetworkPartition is always co-enabled with any other event.
        case (Unique(p : NetworkPartition, _), _) => true
        case (_, Unique(p : NetworkPartition, _)) => true
        // NetworkUnpartition is always co-enabled with any other event.
        case (Unique(p : NetworkUnpartition, _), _) => true
        case (_, Unique(p : NetworkUnpartition, _)) => true
        case (Unique(m1 : MsgEvent, _), Unique(m2 : MsgEvent, _)) =>
          if (m1.receiver != m2.receiver)
            return false
          if (quiescentPeriod.get(earlier).get != quiescentPeriod.get(later).get) {
            return false
          }
          val earlierN = (depGraph get earlier)
          val laterN = (depGraph get later)

          val coEnabeled = laterN.pathTo(earlierN) match {
            case None => true
            case _ => false
          }

          return coEnabeled
      }

      /*
       * For every event in the trace (called later),
       * see if there is some earlier event, such that:
       *
       * 0) They belong to the same receiver.
       * 1) They are co-enabled.
       * 2) Such interleaving hasn't been explored before.
       */
      if (!skipBacktrackComputation) {
        log.trace(Console.GREEN+ "Computing backtrack points. This may take awhile..." + Console.RESET)
        for(laterI <- 0 to trace.size - 1) {
          val later @ Unique(laterEvent, laterID) = getEvent(laterI, trace)

          for(earlierI <- 0 to laterI - 1) {
            val earlier @ Unique(earlierEvent, earlierID) = getEvent(earlierI, trace)

            if (isCoEnabeled(earlier, later)) {
              analyze_dep(earlierI, laterI, trace) match {
                case Some((branchI, needToReplayV)) =>
                  // Since we're exploring an already executed trace, we can
                  // safely mark the interleaving of (earlier, later) as
                  // already explored.
                  backTrack.enqueue((branchI, (later, earlier), needToReplayV))
                case None => // Nothing
              }
            }
          }
        }
      }

      def getNext() : Option[(Int, (Unique, Unique), Seq[Unique])] = {
        // If the backtrack set is empty, this means we're done.
        if (backTrack.isEmpty ||
            (should_cap_distance && backtrackHeuristic.getDistance(backTrack.head) >= stop_at_distance)) {
          log.info("Tutto finito!")
          done(depGraph)
          return None
        }

        backtrackHeuristic.clearKey(backTrack.head)
        val (maxIndex, (e1, e2), replayThis) = backTrack.dequeue

        exploredTracker.isExplored((e1, e2)) match {
          case true =>
            log.debug("Skipping backTrack point: " + e1 + " " + e2)
            return getNext()
          case false => return Some((maxIndex, (e1, e2), replayThis))
        }
      }

      getNext() match {
        case Some((maxIndex, (e1, e2), replayThis)) =>
          log.info(Console.RED + "Exploring a new message interleaving " +
             e1 + " and " + e2  + " at index " + maxIndex + Console.RESET)

          exploredTracker.setExplored(maxIndex, (e1, e2))
          // TODO(cs): the following optimization is not correct if we don't
          // explore in depth-first order. Figure out how to make it correct.
          //exploredTracker.trimExplored(maxIndex)
          //exploredTracker.printExplored()

          if (skipBacktrackComputation) {
            return Some(new Queue[Unique] ++ replayThis)
          } else {
            return Some(trace.take(maxIndex + 1) ++ replayThis)
          }
        case None =>
          return None
      }
    } // } depGraph.synchronized
  }

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }

  // Pre: test is only ever invoked with the same events. Sounds weird, but
  // check out minimization/IncrementalDeltaDebugging.scala.
  def test(events: Seq[ExternalEvent],
           violation_fingerprint: ViolationFingerprint,
           _stats: MinimizationStats,
           init:Option[()=>Any]=None) : Option[EventTrace] = {
    assert(_initialTrace != null)
    if (stopIfViolationFound && shortestTraceSoFar != null) {
      log.warn("Already have shortestTrace!")
      return Some(DPORwHeuristicsUtil.convertToEventTrace(shortestTraceSoFar,
                  events))
    }
    currentSubsequence = events
    lookingFor = violation_fingerprint
    stats = _stats
    Instrumenter().scheduler = this
    if (_initialDepGraph != null) {
      // Since the graph does reference equality, need to reset _root to the
      // root in the graph.
      def matchesRoot(n: Unique) : Boolean = {
        return n.event == MsgEvent("null", "null", null)
      }
      _initialDepGraph.nodes.toList.find(matchesRoot _) match {
        case Some(root) => _root = root.value
        case _ => throw new IllegalArgumentException("No root in initialDepGraph")
      }
    }

    var traceSem = new Semaphore(0)
    var initialTrace = if (startFromBackTrackPoints && !backTrack.isEmpty)
      dpor(currentTrace) else Some(_initialTrace)
    // N.B. reset() and Instrumenter().reinitialize_system
    // are invoked at the beginning of run(), hence we don't need to clean up
    // after ourselves at the end of test(), *unless* some other scheduler
    // besides DPORwHeuristics is going to use the actor system after this.
    stopWatch.start
    run(events,
        f2 = (graph) => {
          if (_initialDepGraph != null) _initialDepGraph ++= graph
          traceSem.release
        },
        initialTrace=initialTrace,
        initialGraph=if (_initialDepGraph != null) Some(_initialDepGraph) else Some(depGraph))
    traceSem.acquire()
    log.debug("Returning from test("+stop_at_distance+")")
    if (foundLookingFor) {
      return Some(DPORwHeuristicsUtil.convertToEventTrace(currentTrace,
                  events))
    } else {
      return None
    }
  }
}

object DPORwHeuristicsUtil {
  def convertToEventTrace(trace: Queue[Unique], events: Seq[ExternalEvent]) : EventTrace = {
    // Convert currentTrace to a format that ReplayScheduler will understand.
    // First, convert Start()'s into SpawnEvents and Send()'s into MsgSends.
    val toReplay = new EventTrace
    toReplay.setOriginalExternalEvents(events)
    events foreach {
      case Start(propCtor, name) =>
        toReplay += SpawnEvent("", propCtor(), name, null)
      case _ => None
    }
    // Then, convert Unique(MsgEvents) to UniqueMsgEvents.
    trace foreach {
      case Unique(m : MsgEvent, id) =>
        if (id != 0) { // Not root
          toReplay += UniqueMsgSend(MsgSend(m.sender,m.receiver,m.msg), id)
          toReplay += UniqueMsgEvent(m, id)
        }
      case _ =>
        // Probably no need to consider Kills or Partitions
        None
    }
    return toReplay
  }

  def convertToDPORTrace(trace: EventTrace, ignoreQuiescence:Boolean=true) : Seq[ExternalEvent] = {
    var allActors = trace flatMap {
      case SpawnEvent(_,_,name,_) => Some(name)
      case _ => None
    }
    return convertToDPORTrace(trace.original_externals, allActors, ignoreQuiescence)
  }

  // Convert externals to a format DPOR will understand
  def convertToDPORTrace(externals: Seq[ExternalEvent], allActors: Iterable[String],
                         ignoreQuiescence: Boolean): Seq[ExternalEvent] = {
    // Convert Trace to a format DPOR will understand. Start by getting a list
    // of all actors, to be used for Kill events.

    // Verify crash-stop, not crash-recovery
    val allActorsSet = allActors.toSet
    assert(allActors.size == allActorsSet.size)

    val filtered_externals = externals flatMap {
      // TODO(cs): DPOR ignores Start after we've invoked setActorNameProps... Ignore them here?
      case s: Start => Some(s)
      case s: Send => Some(s)
      // Convert the following externals into Unique's, since DPORwHeuristics
      // needs ids to match them up correctly.
      case w: WaitQuiescence =>
        if (ignoreQuiescence) {
          None
        } else {
          Some(Unique(w, id=w._id))
        }
      case k @ Kill(name) =>
        Some(Unique(NetworkPartition(Set(name), allActorsSet), id=k._id))
      case p @ Partition(a,b) =>
        Some(Unique(NetworkPartition(Set(a), Set(b)), id=p._id))
      case u @ UnPartition(a,b) =>
        Some(Unique(NetworkUnpartition(Set(a), Set(b)), id=u._id))
      case _ => None
    }
    return filtered_externals
  }
}
