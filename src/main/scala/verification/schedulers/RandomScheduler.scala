package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}
import akka.actor.FSM,
       akka.actor.FSM.Timer


import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable.Set
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Random

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

/**
 * Takes a list of ExternalEvents as input, and explores random interleavings
 * of internal messages until either a maximum number of interleavings is
 * reached, or a given invariant is violated.
 *
 * If invariant_check_interval is <=0, only checks the invariant at the end of
 * the execution. Otherwise, checks the invariant every
 * invariant_check_interval message deliveries.
 *
 * max_executions determines how many executions we will try before giving
 * up.
 *
 * Additionally records internal and external events that occur during
 * executions that trigger violations.
 */
class RandomScheduler(max_executions: Int, enableFailureDetector: Boolean,
                      invariant_check_interval: Int,
                      disableCheckpointing: Boolean)
    extends AbstractScheduler with ExternalEventInjector[ExternalEvent] with TestOracle {
  def this(max_executions: Int) = this(max_executions, true, 0, false)
  def this(max_executions: Int, enableFailureDetector: Boolean) =
      this(max_executions, enableFailureDetector, 0, false)

  def getName: String = "RandomScheduler"

  // Allow the user to place a bound on how many messages are delivered.
  // Useful for dealing with non-terminating systems.
  var maxMessages = Int.MaxValue
  def setMaxMessages(_maxMessages: Int) = {
    maxMessages = _maxMessages
  }

  // TODO(cs): put this into ExternalEventInjector?
  var depGraph = Graph[Unique, DiEdge]()
  private[this] val _root = Unique(MsgEvent("null", "null", null), 0)
  def getRootEvent() : Unique = {
    addGraphNode(_root)
    return _root
  }

  var parentEvent = getRootEvent()
  assert(parentEvent != null)

  var test_invariant : Invariant = null

  // TODO(cs): separate enableFailureDetector and disableCheckpointing out
  // into a config object, passed in to all schedulers..
  if (!enableFailureDetector) {
    disableFailureDetector()
  }

  if (!disableCheckpointing) {
    enableCheckpointing()
  }

  // Current set of enabled events.
  // First element of tuple is the receiver
  var pendingInternalEvents = new RandomizedHashSet[Uniq[(ActorCell,Envelope)]]

  // Current set of externally injected events, to be delivered in the order
  // they arrive.
  var pendingExternalEvents = new Queue[Uniq[(ActorCell, Envelope)]]

  // The violation we're looking for, if not None.
  var lookingFor : Option[ViolationFingerprint] = None

  // If we're looking for a specific violation, this is just used a boolean
  // flag: if not None, then we've found what we're looking for.
  // Otherwise, it will contain the first safety violation we found.
  var violationFound : Option[ViolationFingerprint] = None

  // The trace we're exploring
  var trace : Seq[ExternalEvent] = null

  enableCheckpointing()

  // how many non-checkpoint messages we've scheduled so far.
  var messagesScheduledSoFar = 0

  // what was the last value of messagesScheduledSoFar we took a checkpoint at.
  var lastCheckpoint = 0

  var stats: MinimizationStats = null

  /**
   * If we're looking for a specific violation, return None if the given
   * violation doesn't match, or Some(violation) if it does.
   *
   * If we're not looking for a specific violation, return the given
   * violation.
   */
  private[this] def violationMatches(violation: Option[ViolationFingerprint]) : Option[ViolationFingerprint] = {
    lookingFor match {
      case None =>
        return violation
      case Some(original_fingerprint) =>
        violation match {
          case None =>
            return None
          case Some(fingerprint) =>
            if (original_fingerprint.matches(fingerprint)) {
              return lookingFor
            } else {
              return None
            }
        }
    }
  }

  private[this] def setParentEvent (event: Unique) {
    assert(event != null)
    val graphNode = depGraph.get(event)
    val rootNode = depGraph.get(getRootEvent())
    // val pathLength = graphNode.pathTo(rootNode) match {
    //   case Some(p) => p.length
    //   case _ =>
    //     throw new Exception("Unexpected path")
    // }
    parentEvent = event
  }

  private[this] def addGraphNode (event: Unique) = {
    depGraph.add(event)
  }

  // Convert RandomScheduler's Uniq to depGraph's Unique, using the
  // same id.
  def getUniqueFromUniq(uniq : Uniq[(ActorCell, Envelope)]) : Unique = {
    val cell = uniq.element._1
    val envelope = uniq.element._2
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgEvent = new MsgEvent(snd, rcv, envelope.message)
    return Unique(msgEvent, id=uniq.id)
  }

  /**
   * Given a message, figure out if we have already seen
   * it before. We achieve this by consulting the
   * dependency graph.
   *
   * * @param (cell, envelope): Original message context.
   */
  // TODO(cs): redundant with DPORwHeuristics.
  def updateDepGraph(uniq : Uniq[(ActorCell, Envelope)]) {
    val newUnique = getUniqueFromUniq(uniq)
    val parent = parentEvent match {
      case u @ Unique(m: MsgEvent, id) => u
      case _ => throw new Exception("parent event not a message:" + parentEvent)
    }
    val inNeighs = depGraph.get(parent).inNeighbors

    def matchMessage (event: Event) : Boolean = {
      // Ugly hack since TimeoutMarker is private in new enough (> 2.0) Akka versions.
      return (event, newUnique.event) match {
        case (MsgEvent(s1, r1, Timer(n1, m1, rep1, _)), MsgEvent(s2, r2, Timer(n2, m2, rep2, _))) =>
          (s1 == s2) && (r1 == r2) && (n1 == n2) && (m1 == m2) && (rep1 == rep2)
        case (MsgEvent(_, rcv1, m1), MsgEvent(_, rcv2, m2)) =>
          (ClassTag(m1.getClass).toString, ClassTag(m2.getClass).toString) match {
            case ("akka.actor.FSM$TimeoutMarker", "akka.actor.FSM$TimeoutMarker") => rcv1 == rcv2
            case _ => event == newUnique.event
          }
        case _ =>
          event == newUnique.event
      }
    }

    val unique =
      inNeighs.find { x => matchMessage(x.value.event) } match {
        case Some(x) =>
          // Ensure that the id's match in the future, by mutating uniq
          // TODO(cs): not sure this is completely correct
          uniq.id = x.value.id
          x.value
        case None =>
          newUnique
        case _ => throw new Exception("wrong type")
    }

    addGraphNode(unique)
    depGraph.addEdge(unique, parentEvent)(DiEdge)
  }

  /**
   * Given an external event trace, randomly explore executions involving those
   * external events.
   *
   * Returns a trace of the internal and external events observed if a failing
   * execution was found, along with a `fingerprint` of the safety violation.
   * otherwise returns None if no failure was triggered within max_executions.
   *
   * Callers should call shutdown() sometime after this method returns if they
   * want to invoke any other methods.
   *
   * Precondition: setInvariant has been invoked.
   */
  def explore (_trace: Seq[ExternalEvent]) : Option[(EventTrace, ViolationFingerprint)] = {
    return explore(_trace, None)
  }

  /**
   * if looking_for is not None, only look for an invariant violation that
   * matches looking_for
   */
  def explore (_trace: Seq[ExternalEvent],
               _lookingFor: Option[ViolationFingerprint]) : Option[(EventTrace, ViolationFingerprint)] = {
    if (!(Instrumenter().scheduler eq this)) {
      throw new IllegalStateException("Instrumenter().scheduler not set!")
    }
    trace = _trace
    lookingFor = _lookingFor

    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }

    for (i <- 1 to max_executions) {
      println("Trying random interleaving " + i)
      event_orchestrator.events.setOriginalExternalEvents(_trace)
      if (stats != null) {
        stats.increment_replays()
      }
      val event_trace = execute_trace(_trace)

      // If the violation has already been found, return.
      violationFound match {
        case Some(fingerprint) =>
          return Some((event_trace, fingerprint))
        // Else, check the invariant condition one last time.
        case None =>
          if (!disableCheckpointing) {
            var checkpoint : HashMap[String, Option[CheckpointReply]] = null
            checkpoint = takeCheckpoint()
            val violation = test_invariant(_trace, checkpoint)
            violationFound = violationMatches(violation)
            violationFound match {
              case Some(fingerprint) =>
                return Some((event_trace, fingerprint))
              case None => None
            }
          }
      }

      if (i != max_executions) {
        // 'Tis a lesson you should heed: Try, try, try again.
        // If at first you don't succeed: Try, try, try again
        reset_all_state
      }
    }
    // No bug found...
    return None
  }

  override def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    val uniq = Uniq[(ActorCell, Envelope)]((cell, envelope))

    event_orchestrator.events.appendMsgSend(snd, rcv, envelope.message, uniq.id)

    handle_event_produced(snd, rcv, envelope) match {
      case InternalMessage => {
        updateDepGraph(uniq)
        if (!crosses_partition(snd, rcv)) {
          pendingInternalEvents.insert(uniq)
        }
      }
      case ExternalMessage => {
        updateDepGraph(uniq)
        // We assume that the failure detector and the outside world always
        // have connectivity with all actors, i.e. no failure detector partitions.
        pendingExternalEvents += uniq
      }
      case SystemMessage => None
      case CheckpointReplyMessage =>
        if (checkpointer.done && !blockedOnCheckpoint.get) {
          val violation = test_invariant(trace, checkpointer.checkpoints)
          require(violationFound == None)
          violationFound = violationMatches(violation)
        }
    }
  }

  // Record a mapping from actor names to actor refs
  override def event_produced(event: Event) = {
    super.event_produced(event)
    handle_spawn_produced(event)
  }

  // Record that an event was consumed
  override def event_consumed(event: Event) = {
    handle_spawn_consumed(event)
  }

  // Record a message send event
  override def event_consumed(cell: ActorCell, envelope: Envelope) = {
    handle_event_consumed(cell, envelope)
  }

  override def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    // First, check if we've found the violation. If so, stop.
    violationFound match {
      case Some(fingerprint) =>
        return None
      case None =>
        None
    }

    // Also check if we've exceeded our message limit
    if (messagesScheduledSoFar > maxMessages) {
      println("Exceeded maxMessages")
      numWaitingFor.set(0)
      event_orchestrator.finish_early
      return None
    }

    // Otherwise, see if it's time to check the invariant violation.
    if (invariant_check_interval > 0 &&
        (messagesScheduledSoFar % invariant_check_interval) == 0 &&
        !blockedOnCheckpoint.get() &&
        lastCheckpoint != messagesScheduledSoFar) {
      // N.B. we check the invariant once we have received all
      // CheckpointReplies.
      println("Checking invariant")
      lastCheckpoint = messagesScheduledSoFar
      // TODO(cs): remove any elements in pendingExternalEvents, and move them
      // to the end of pendingExternalEvents once the CheckpointRequests have
      // been queued. Not strictly necessary for correctness, just currently means that
      // we sometimes collect the checkpoint a bit later than we want to.
      prepareCheckpoint()
    }

    // Proceed normally.
    send_external_messages()
    // Always prioritize external events.
    if (!pendingExternalEvents.isEmpty) {
      val uniq = pendingExternalEvents.dequeue()
      event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
      uniq.element._2.message match {
        case CheckpointRequest => None
        case _ =>
          messagesScheduledSoFar += 1
          if (messagesScheduledSoFar == Int.MaxValue) {
            messagesScheduledSoFar = 1
          }
      }
      val unique = getUniqueFromUniq(uniq)
      (depGraph get unique)
      setParentEvent(unique)
      return Some(uniq.element)
    }

    // Do we have some pending events
    if (pendingInternalEvents.isEmpty) {
      return None
    }

    messagesScheduledSoFar += 1
    if (messagesScheduledSoFar == Int.MaxValue) {
      messagesScheduledSoFar = 1
    }
    val uniq = pendingInternalEvents.removeRandomElement()
    event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)

    val unique = getUniqueFromUniq(uniq)
    (depGraph get unique)
    setParentEvent(unique)
    return Some(uniq.element)
  }

  override def notify_quiescence () {
    violationFound match {
      case None => handle_quiescence
      case Some(fingerprint) =>
        // Wake up the main thread early; no need to continue with the rest of
        // the trace.
        println("Violation found early. Halting")
        started.set(false)
        traceSem.release()
    }
  }

  // Shutdown the scheduler, this ensures that the instrumenter is returned to its
  // original pristine form, so one can change schedulers
  override def shutdown () = {
    handle_shutdown
  }

  // Notification that the system has been reset
  override def start_trace() : Unit = {
    handle_start_trace
  }

  override def before_receive(cell: ActorCell) : Unit = {
    handle_before_receive(cell)
  }

  override def after_receive(cell: ActorCell) : Unit = {
    handle_after_receive(cell)
  }

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }

  override def reset_all_state () {
    // TODO(cs): also reset Instrumenter()'s state?
    reset_state
    // N.B. important to clear our state after we invoke reset_state, since
    // it's possible that enqueue_message may be called during shutdown.
    super.reset_all_state
    pendingInternalEvents = new RandomizedHashSet[Uniq[(ActorCell, Envelope)]]
    pendingExternalEvents = new Queue[Uniq[(ActorCell, Envelope)]]
    lookingFor = None
    violationFound = None
    trace = null
    messagesScheduledSoFar = 0
    lastCheckpoint = 0
    depGraph = Graph[Unique, DiEdge]()
    setParentEvent(getRootEvent())
  }

  def test(events: Seq[ExternalEvent],
           violation_fingerprint: ViolationFingerprint,
           _stats: MinimizationStats) : Option[EventTrace] = {
    stats = _stats
    Instrumenter().scheduler = this
    val tuple_option = explore(events, Some(violation_fingerprint))
    reset_all_state
    // test passes if we were unable to find a failure.
    tuple_option match {
      case Some((trace, violation)) =>
        return Some(trace)
      case None =>
        return None
    }
  }
}
