package akka.dispatch.verification

import akka.actor.ActorCell,
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

       
import com.typesafe.scalalogging.LazyLogging,
       org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger


/**
 * DPOR scheduler.
 *   - prioritizePendingUponDivergence: if true, whenever an expected message in
 *     nextTrace does not show up, rather than picking a random pending event
 *     to proceed, instead pick the pending event that appears first in
 *     nextTrace.
 *   - If invariant_check_interval is <=0, only checks the invariant at the end of
 *     the execution. Otherwise, checks the invariant every
 *     invariant_check_interval message deliveries.
 */
class DPORwHeuristics(enableCheckpointing: Boolean,
  messageFingerprinter: FingerprintFactory,
  prioritizePendingUponDivergence:Boolean=false,
  invariant_check_interval:Int=0,
  depth_bound: Option[Int] = None) extends Scheduler with LazyLogging with TestOracle {
  def this() = this(false, new FingerprintFactory, false, 0, None)
  
  final val SCHEDULER = "__SCHEDULER__"
  final val PRIORITY = "__PRIORITY__"
  type Trace = Queue[Unique]

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

  // Collection of all actors that could possibly have messages sent to them.
  var actorNameProps : Option[Seq[Tuple2[Props,String]]] = None
  def setActorNameProps(actorNamePropPairs: Seq[Tuple2[Props,String]]) {
    actorNameProps = Some(actorNamePropPairs)
  }
    
  var instrumenter = Instrumenter
  var externalEventList : Seq[ExternalEvent] = Vector()
  var externalEventIdx = 0
  var started = false
  
  var currentTime = 0
  var interleavingCounter = 0
  
  val pendingEvents = new HashMap[String, Queue[(Unique, ActorCell, Envelope)]]  
  val actorNames = new HashSet[String]
 
  val depGraph = Graph[Unique, DiEdge]()

  val quiescentPeriod = new HashMap[Unique, Int]

  // Change the Ordering to try different ordering heuristics, currently exploration is in descending order of depth
  val backTrack = new PriorityQueue[(Int, (Unique, Unique), List[Unique])]()(
    Ordering.by[(Int, (Unique, Unique), List[Unique]), Int](_._1))
  var invariant : Queue[Unique] = Queue()
  var exploredTracker = new ExploredTacker
  
  val currentTrace = new Trace
  val nextTrace = new Trace
  var parentEvent = getRootEvent
  var currentDepth = 0
  
  val partitionMap = new HashMap[String, HashSet[String]]
  
  var post: (Trace) => Unit = nullFunPost
  var done: (Graph[Unique, DiEdge]) => Unit = nullFunDone

  private[this] var currentRoot = getRootEvent
  
  var currentQuiescentPeriod = 0
  var awaitingQuiescence = false
  var nextQuiescentPeriod = 0
  var quiescentMarker:Unique = null

  var test_invariant : Invariant = null
  // Whether we are currently awaiting checkpoint responses from actors.
  var blockedOnCheckpoint = new AtomicBoolean(false)
  var stats: MinimizationStats = null
  def getName: String = "DPORwHeuristics"
  var currentSubsequence : Seq[ExternalEvent] = null
  var lookingFor : ViolationFingerprint = null
  var foundLookingFor = false
  var _initialTrace : Trace = null
  def setInitialTrace(t: Trace) {
    _initialTrace = t
  }
  var _initialDegGraph : Graph[Unique, DiEdge] = null
  def setInitialDepGraph(g: Graph[Unique, DiEdge]) {
    _initialDegGraph = g
  }
  var checkpointer : CheckpointCollector =
    if (enableCheckpointing) new CheckpointCollector else null

  def nullFunPost(trace: Trace) : Unit = {}
  def nullFunDone(graph: Graph[Unique, DiEdge]) : Unit = {}

  // Reset state between runs
  private[this] def reset() = {
    pendingEvents.clear()
    currentDepth = 0
    currentTrace.clear
    nextTrace.clear
    partitionMap.clear
    awaitingQuiescence = false
    nextQuiescentPeriod = 0
    quiescentMarker = null
    currentQuiescentPeriod = 0
    depGraph.clear
    externalEventIdx = 0
    currentTime = 0
    interleavingCounter = 0
    actorNames.clear
    quiescentPeriod.clear
    backTrack.clear
    exploredTracker.clear
    blockedOnCheckpoint.set(false)
    currentRoot = getRootEvent

    setParentEvent(getRootEvent)
  }
  
  private[this] def awaitQuiescenceUpdate (nextEvent:Unique) = { 
    logger.trace(Console.BLUE + "Beginning to wait for quiescence " + Console.RESET)
    nextEvent match {
      case Unique(WaitQuiescence(), id) =>
        awaitingQuiescence = true
        nextQuiescentPeriod = id
        quiescentMarker = nextEvent
      case _ =>
        throw new Exception("Bad args")
    }
  }

  private[this] def setParentEvent (event: Unique) {
    val graphNode = depGraph.get(event)
    val rootNode = depGraph.get(getRootEvent)
    val pathLength = graphNode.pathTo(rootNode) match {
      case Some(p) => p.length
      case _ => 
        throw new Exception("Unexpected path")
    }
    parentEvent = event
    currentDepth = pathLength + 1
    
  }

  private[this] def addGraphNode (event: Unique) = {
    depGraph.add(event)
    quiescentPeriod(event) = currentQuiescentPeriod
  }
  
  private[this] var _root = Unique(MsgEvent("null", "null", null), 0)
  def getRootEvent() : Unique = {
    addGraphNode(_root)
    _root
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
  def mutableTraceIterator( trace: Trace) : Option[Unique] =
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

  
  
  
  // Figure out what is the next message to schedule.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    
    
    def checkInvariant[T1](result : Option[T1]) = result match {
    
      case Some((Unique(_, nID), _, _)) => invariant.headOption match {
          case Some(Unique(_, invID)) if (nID == invID) =>
            logger.trace( Console.RED + "Managed to replay the intended message: "+ nID + Console.RESET )
            invariant.dequeue()
          case _ =>
        }
        
      case _ =>
    }
    
    
    // Find equivalent messages to the one we are currently looking for.
    def equivalentTo(u1: Unique, other: (Unique, ActorCell, Envelope)) : 
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
    def getPendingEvent(): Option[(Unique, ActorCell, Envelope)] = {
      // Do we have some pending events
      Util.dequeueOne(pendingEvents) match {
        case Some( next @ (Unique(MsgEvent(snd, rcv, msg), id), _, _)) =>
          logger.trace( Console.GREEN + "Now playing pending: " 
              + "(" + snd + " -> " + rcv + ") " +  + id + Console.RESET )
          Some(next)
          
        case Some(par @ (Unique(NetworkPartition(part1, part2), id), _, _)) =>
          logger.trace( Console.GREEN + "Now playing the high level partition event " +
              id + Console.RESET)
          Some(par)

        case Some(par @ (Unique(NetworkUnpartition(part1, part2), id), _, _)) =>
          logger.trace( Console.GREEN + "Now playing the high level partition event " +
              id + Console.RESET)
          Some(par)

        case Some(qui @ (Unique(WaitQuiescence(), id), _, _)) =>
          logger.trace( Console.GREEN + "Now playing the high level quiescence event " +
              id + Console.RESET)
          Some(qui)

        case None => None        
        case _ => throw new Exception("internal error")
      }
    }
    
    
    def getMatchingMessage() : Option[(Unique, ActorCell, Envelope)] = {
      getNextTraceMessage() match {
        // The trace says there is a message event to run.
        case Some(u @ Unique(MsgEvent(snd, rcv, msg), id)) =>

          // Look at the pending events to see if such message event exists. 
          pendingEvents.get(rcv) match {
            case Some(queue) => queue.dequeueFirst(equivalentTo(u, _))
            case None =>  None
          }
          
        case Some(u @ Unique(NetworkPartition(_, _), id)) =>
          
          // Look at the pending events to see if such message event exists. 
          pendingEvents.get(SCHEDULER) match {
            case Some(queue) => queue.dequeueFirst(equivalentTo(u, _))
            case None =>  None
          }
          
        case Some(u @ Unique(NetworkUnpartition(_, _), id)) =>
          
          // Look at the pending events to see if such message event exists. 
          pendingEvents.get(SCHEDULER) match {
            case Some(queue) => queue.dequeueFirst(equivalentTo(u, _))
            case None =>  None
          }

        case Some(u @ Unique(WaitQuiescence(), _)) => // Look at the pending events to see if such message event exists. 
          pendingEvents.get(SCHEDULER) match {
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
    def getNextMatchingMessage(): Option[(Unique, ActorCell, Envelope)] = {
      var foundMatching : Option[(Unique, ActorCell, Envelope)] = None
      while (!nextTrace.isEmpty && foundMatching == None) {
        foundMatching = getMatchingMessage()
      }
      return foundMatching
    }

    //
    // Are there any prioritized events that need to be dispatched.
    pendingEvents.get(PRIORITY) match {
      case Some(queue) if !queue.isEmpty => {
        val (_, cell, env) = queue.dequeue()
        return Some((cell, env))
      }
      case _ => None
    }
    
    
    val result = awaitingQuiescence match {
      case false =>
        val matching = prioritizePendingUponDivergence match {
          case true => getNextMatchingMessage
          case false => getMatchingMessage
        }

        matching match {
          
          // There is a pending event that matches a message in our trace.
          // We call this a convergent state.
          case m @ Some((u @ Unique(MsgEvent(snd, rcv, msg), id), cell, env)) =>
            logger.trace( Console.GREEN + "Replaying the exact message: Message: " +
                "(" + snd + " -> " + rcv + ") " +  + id + Console.RESET )
            Some((u, cell, env))
            
          case Some((u @ Unique(NetworkPartition(part1, part2), id), _, _)) =>
            logger.trace( Console.GREEN + "Replaying the exact message: Partition: (" 
                + part1 + " <-> " + part2 + ")" + Console.RESET )
            Some((u, null, null))

          case Some((u @ Unique(NetworkUnpartition(part1, part2), id), _, _)) =>
            logger.trace( Console.GREEN + "Replaying the exact message: Unpartition: (" 
                + part1 + " <-> " + part2 + ")" + Console.RESET )
            Some((u, null, null))


          case Some((u @ Unique(WaitQuiescence(), id), _, _)) =>
            logger.trace( Console.GREEN + "Replaying the exact message: Quiescence: (" 
                + id +  ")" + Console.RESET )
            Some((u, null, null))
            
          // We call this a divergent state.
          case None => getPendingEvent()
          
          // Something went wrong.
          case _ => throw new Exception("not a message")
        }
      case true => // Don't call getMatchingMessage when waiting quiescence. Except when divergent or running the first
                  // time through, there should be no pending messages, signifying quiescence. Get pending event takes
                  // care of the first run. We could explicitly detect divergence here, but we haven't been so far.
        getPendingEvent()
    }
    
    
    
    checkInvariant(result)
    
    result match {
      
      case Some((nextEvent @ Unique(MsgEvent(snd, rcv, msg), nID), cell, env)) =>
        partitionMap.get(snd) match {
          case Some(set) =>
            if (set.contains(rcv)) {
              logger.trace(Console.RED + "Discarding event " + nextEvent + " due to partition ")
              return schedule_new_message()
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
        return schedule_new_message()

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
        return schedule_new_message()

      case Some((nextEvent @ Unique(WaitQuiescence(), nID), _, _)) =>
        awaitQuiescenceUpdate(nextEvent)
        return schedule_new_message()

      case _ =>
        return None
    }
    
  }
  
  
  def next_event() : Event = {
    throw new Exception("not implemented next_event()")
  }

  
  // Record that an event was consumed
  def event_consumed(event: Event) = {
  }
  
  
  def event_consumed(cell: ActorCell, envelope: Envelope) = {
  }
  
  
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

    if (enableCheckpointing) {
      checkpointer.startCheckpointCollector(Instrumenter().actorSystem)
    }

    //actorNames.map(name =>
    //  partitionMap(name) = partitionMap.getOrElse(name,
    //    new HashSet[String]) ++ actorNames
    //)
  }
  
  def runExternal() = {
    logger.trace(Console.RED + " RUN EXTERNAL CALLED initial IDX = " + externalEventIdx +Console.RESET) 
   
    var await = false
    while (externalEventIdx < externalEventList.length && !await) {
      val event = externalEventList(externalEventIdx)
      event match {
    
        case Start(propsCtor, name) => 
          // If not already started: start it and unisolate it
          if (!(instrumenter().actorMappings contains name)) {
            instrumenter().actorSystem().actorOf(propsCtor(), name)
            //partitionMap -= name
          }
    
        case Send(rcv, msgCtor) =>
          val ref = instrumenter().actorMappings(rcv)
          logger.trace(Console.BLUE + " sending " + rcv + " messge " + msgCtor() + Console.RESET)
          instrumenter().actorMappings(rcv) ! msgCtor()

        case uniq @ Unique(par : NetworkPartition, id) =>  
          val msgs = pendingEvents.getOrElse(SCHEDULER, new Queue[(Unique, ActorCell, Envelope)])
          pendingEvents(SCHEDULER) = msgs += ((uniq, null, null))
          addGraphNode(uniq)

        case uniq @ Unique(par : NetworkUnpartition, id) =>  
          val msgs = pendingEvents.getOrElse(SCHEDULER, new Queue[(Unique, ActorCell, Envelope)])
          pendingEvents(SCHEDULER) = msgs += ((uniq, null, null))
          addGraphNode(uniq)
       
       
        case event @ Unique(WaitQuiescence(), _) =>
          val msgs = pendingEvents.getOrElse(SCHEDULER, new Queue[(Unique, ActorCell, Envelope)])
          pendingEvents(SCHEDULER) = msgs += ((event, null, null))
          addGraphNode(event)
          await = true

        // A unique ID needs to be associated with all network events.
        case par : NetworkPartition => throw new Exception("internal error")
        case par : NetworkUnpartition => throw new Exception("internal error")
        case _ => throw new Exception("unsuported external event " + event)
      }
      externalEventIdx += 1
    }
    
    logger.trace(Console.RED + " RUN EXTERNAL LOOP ENDED idx = " + externalEventIdx + Console.RESET) 
    
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
      case WaitQuiescence() =>
        Unique(WaitQuiescence())
      case other => other
    } }
    
    post = f1
    done = f2
    
    initialTrace match {
      case Some(tr) => 
        nextTrace ++= tr
        initialGraph match {
          case Some(gr) => depGraph ++= gr
          case _ => throw new Exception("Must supply a dependency graph with a trace")
        } 
      case _ => nextTrace.clear
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
  def getMessage(cell: ActorCell, envelope: Envelope) : Unique = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = new MsgEvent(snd, rcv, messageFingerprinter.fingerprint(envelope.message))
    // Who cares if the parentEvent is in fact a message, as long as it is a parent.
    val parent = parentEvent

    def matchMessage (event: Event) : Boolean = {
      return event == msg
    }

    val inNeighs = depGraph.get(parent).inNeighbors
    inNeighs.find { x => matchMessage(x.value.event) } match {
      
      case Some(x) => return x.value
      case None =>
        val newMsg = Unique( MsgEvent(msg.sender, msg.receiver, msg.msg) )
        logger.trace(
            Console.YELLOW + "Not seen: " + newMsg.id + 
            " (" + msg.sender + " -> " + msg.receiver + ") " + msg.msg + Console.RESET)
        return newMsg
      case _ => throw new Exception("wrong type")
    }
      
  }
  
  
  
  def event_produced(cell: ActorCell, envelope: Envelope) : Unit = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    if (rcv == CheckpointSink.name) {
      assert(enableCheckpointing)
      checkpointer.handleCheckpointResponse(envelope.message, snd)
      // Don't add anything to pendingEvents, since we just "delivered" it
      return
    }

    envelope.message match {
    
      // CheckpointRequests and failure detector messeages are simply enqueued to the priority queued
      // and dispatched at the earliest convenience.
      case NodesReachable | NodesUnreachable | CheckpointRequest =>
        val msgs = pendingEvents.getOrElse(PRIORITY, new Queue[(Unique, ActorCell, Envelope)])
        pendingEvents(PRIORITY) = msgs += ((null, cell, envelope))
      
      case _ =>
        val unique @ Unique(msg : MsgEvent, id) = getMessage(cell, envelope)
        val msgs = pendingEvents.getOrElse(msg.receiver, new Queue[(Unique, ActorCell, Envelope)])
        // Do not enqueue if bound hit
        if (!should_bound || currentDepth < stop_at_depth) {
          logger.trace(Console.BLUE + "Enqueuing event " + cell + " " + envelope + " " + unique + 
                       " (" + parentEvent + ") " + Console.RESET)
          pendingEvents(msg.receiver) = msgs += ((unique, cell, envelope))
        } else {
          logger.debug(Console.RED + "Not adding message because we hit depth bound " + Console.RESET)
        }
        
        logger.debug(Console.BLUE + "New event: " +
            "(" + msg.sender + " -> " + msg.receiver + ") " +
            id + Console.RESET)
        
        addGraphNode(unique)
        depGraph.addEdge(unique, parentEvent)(DiEdge)
    }

  }
  
  
  // Called before we start processing a newly received event
  def before_receive(cell: ActorCell) {
  }
  
  // Called after receive is done being processed 
  def after_receive(cell: ActorCell) {
  }

  
  def printPath(path : List[depGraph.NodeT]) : String = {
    var pathStr = ""
    for(node <- path) {
      node.value match {
        case Unique(m : MsgEvent, id) => pathStr += id + " "
        case _ => throw new Exception("internal error not a message")
      }
    }
    return pathStr
  }

  
  
  def notify_quiescence() {
    
    if (awaitingQuiescence) {
      awaitingQuiescence = false
      logger.trace(Console.BLUE + " Done waiting for quiescence " + Console.RESET)

      currentQuiescentPeriod = nextQuiescentPeriod
      nextQuiescentPeriod = 0

      currentTrace += quiescentMarker
      addGraphNode(quiescentMarker)
      depGraph.addEdge(quiescentMarker, currentRoot)(DiEdge)
      currentRoot = quiescentMarker
      setParentEvent(quiescentMarker)
      quiescentMarker = null

      runExternal()
    } else {
      if (enableCheckpointing && test_invariant != null) {
        if (blockedOnCheckpoint.get) {
          // We've finished our checkpoint. Check our invariant. If it fails,
          // stop exploring! Otherwise, continue exploring schedules.
          blockedOnCheckpoint.set(false)
          println("Checking invariant")
          val violation = test_invariant(currentSubsequence, checkpointer.checkpoints)
          violation match {
            case Some(v) =>
              if (lookingFor.matches(v)) {
                println("Found matching violation!")
                foundLookingFor = true
                return
              }
            case None =>
              println("No matching violation. Proceeding...")
          }
        } else {
          // Initiate a checkpoint
          blockedOnCheckpoint.set(true)
          println("Initiating checkpoint..")
          val actorRefs = Instrumenter().actorMappings.
                            filterNot({case (k,v) => ActorTypes.systemActor(k)}).
                            values.toSeq
          val checkpointRequests = checkpointer.prepareRequests(actorRefs)
          // Put our requests at the front of the queue, and any existing requests
          // at the end of the queue.
          for ((actor, request) <- checkpointRequests) {
            Instrumenter().actorMappings(actor) ! request
          }
          Instrumenter().await_enqueue
          Instrumenter().start_dispatch
          return
        }
      }

      logger.info("\n--------------------- Interleaving #" +
                  interleavingCounter + " ---------------------")
      
      logger.debug(Console.BLUE + "Current trace: " +
          Util.traceStr(currentTrace) + Console.RESET)

      for (Unique(ev, id) <- currentTrace) 
        logger.debug(Console.BLUE + " " + id + " " + ev + Console.RESET)

      
      nextTrace.clear()
      
      // Unconditionally post the current trace
      post(currentTrace)
      
      dpor(currentTrace) match {
        case Some(trace) =>
          nextTrace ++= trace
          
          logger.debug(Console.BLUE + "Next trace:  " + 
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
  
  
  def enqueue_message(receiver: String,msg: Any): Unit = {
    logger.trace(Console.BLUE + "Enqueuing timer to " + receiver + " with msg " + msg + Console.RESET)
    instrumenter().actorMappings(receiver) ! msg
    instrumenter().await_enqueue()
  }
  
  
  def shutdown(): Unit = {
    throw new Exception("internal error not a message")
  }

  def notify_timer_cancel (receiver: ActorRef, msg: Any) = {
    logger.trace(Console.BLUE + " Trying to cancel timer for " + receiver.path.name + " " + msg + Console.BLUE)
    def equivalentTo(u: (Unique, ActorCell, Envelope)): Boolean = {
      u._3 match {
        case null => false
        case e => e.message == msg && u._2.self.path.name == receiver.path.name
      }
    }

    pendingEvents.get(receiver.path.name) match {
      case Some(q) =>
        q.dequeueFirst(equivalentTo(_))
        logger.trace(Console.RED + " Removing pending event (" + 
                    receiver.path.name + " , " + msg + ")" + Console.RESET)
      case None => // This cancellation came too late, things have already been done.
    }
  }
  
  
  def getEvent(index: Integer, trace: Trace) : Unique = {
    trace(index) match {
      case u: Unique => u 
      case _ => throw new Exception("internal error not a message")
    }
  }
  

  def dpor(trace: Trace) : Option[Trace] = {
    
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

      // See if this interleaving has been explored.
      //val explored = exploredTracker.isExplored((later, earlier))
      //if (explored) return None

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
          val commonPrefix = laterPath.intersect(earlierPath)

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
      //case (_, _) =>
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
    for(laterI <- 0 to trace.size - 1) {
      val later @ Unique(laterEvent, laterID) = getEvent(laterI, trace)

      for(earlierI <- 0 to laterI - 1) {
        val earlier @ Unique(earlierEvent, earlierID) = getEvent(earlierI, trace) 
        
        if ( isCoEnabeled(earlier, later)) {
          
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
    
    def getNext() : Option[(Int, (Unique, Unique), Seq[Unique])] = {
            // If the backtrack set is empty, this means we're done.
      if (backTrack.isEmpty) {
        logger.info("Tutto finito!")
        done(depGraph)
        return None
        //System.exit(0);
      }
  
      val (maxIndex, (e1, e2), replayThis) = backTrack.dequeue

      exploredTracker.isExplored((e1, e2)) match {
        case true => return getNext()
        case false => return Some((maxIndex, (e1, e2), replayThis))
      }

    }

    getNext() match {
      case Some((maxIndex, (e1, e2), replayThis)) =>
        

        logger.info(Console.RED + "Exploring a new message interleaving " + 
           e1.id + " and " + e2.id  + " at index " + maxIndex + Console.RESET)
            
        
        exploredTracker.setExplored(maxIndex, (e1, e2))
        exploredTracker.trimExplored(maxIndex)
        exploredTracker.printExplored()
        
        // A variable used to figure out if the replay diverged.
        invariant = Queue(e1, e2)
        
        // Return all events up to the backtrack index we're interested in
        // and slap on it a new set of events that need to be replayed in
        // order to explore that interleaving.
        return Some(trace.take(maxIndex + 1) ++ replayThis)
      case None =>
        return None
    }
  }
  

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }

  def test(events: Seq[ExternalEvent],
           violation_fingerprint: ViolationFingerprint,
           _stats: MinimizationStats) : Option[EventTrace] = {
    assert(_initialDegGraph != null)
    assert(_initialTrace != null)
    currentSubsequence = events
    lookingFor = violation_fingerprint
    stats = _stats
    Instrumenter().scheduler = this
    // Since the graph does reference equality, need to reset _root to the
    // root in the graph.
    def matchesRoot(n: Unique) : Boolean = {
      return n.event == MsgEvent("null", "null", null)
    }
    _initialDegGraph.nodes.toList.find(matchesRoot _) match {
      case Some(root) => _root = root.value
      case _ => throw new IllegalArgumentException("No root in initialDepGraph")
    }

    var traceSem = new Semaphore(0)
    run(events,
        f2 = (graph) => {
          _initialDegGraph ++= graph
          traceSem.release
        },
        initialTrace=Some(_initialTrace),
        initialGraph=Some(_initialDegGraph))
    traceSem.acquire()
    reset
    Instrumenter().restart_system()
    if (foundLookingFor) {
      return Some(new EventTrace)
    } else {
      return None
    }
  }
}
