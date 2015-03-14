package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}

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
class RandomScheduler(max_executions: Int,
                      messageFingerprinter: FingerprintFactory,
                      enableFailureDetector: Boolean,
                      invariant_check_interval: Int,
                      disableCheckpointing: Boolean)
    extends AbstractScheduler with ExternalEventInjector[ExternalEvent] with TestOracle {
  def this(max_executions: Int) = this(max_executions, new FingerprintFactory, true, 0, false)
  def this(max_executions: Int, enableFailureDetector: Boolean) =
      this(max_executions, new FingerprintFactory, enableFailureDetector, 0, false)

  def getName: String = "RandomScheduler"

  // Allow the user to place a bound on how many messages are delivered.
  // Useful for dealing with non-terminating systems.
  var maxMessages = Int.MaxValue
  def setMaxMessages(_maxMessages: Int) = {
    maxMessages = _maxMessages
  }

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
  // Our use of Uniq and Unique is somewhat confusing. Uniq is used to
  // associate MsgSends with their subsequent MsgEvents.
  // Unique is used by DepTracker.
  var pendingEvents = new RandomizedHashSet[Tuple2[Uniq[(ActorCell,Envelope)],Unique]]

  // Current set of failure detector or CheckpointRequest messages destined for
  // actors, to be delivered in the order they arrive.
  // Always prioritized over internal messages.
  var pendingSystemMessages = new Queue[Uniq[(ActorCell, Envelope)]]

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

  // How many times we've replayed
  var stats: MinimizationStats = null

  // For every message we deliver, track which messages become enabled as a
  // result of our delivering that message. This can be used later to recreate
  // the DepGraph (used by DPOR).
  var depTracker = new DepTracker(messageFingerprinter)

  // Tell ExternalEventInjector to notify us whenever a WaitQuiescence has just
  // caused us to arrive at Quiescence.
  setQuiescenceCallback(depTracker.reportQuiescence)
  // Tell EventOrchestrator to tell us about Kills, Parititions, UnPartitions
  event_orchestrator.setKillCallback(depTracker.reportKill)
  event_orchestrator.setPartitionCallback(depTracker.reportPartition)
  event_orchestrator.setUnPartitionCallback(depTracker.reportUnPartition)

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
    var snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    val uniq = Uniq[(ActorCell, Envelope)]((cell, envelope))
    var isTimer = false

    handle_event_produced(snd, rcv, envelope) match {
      case InternalMessage => {
        if (snd == "deadLetters") {
          isTimer = true
        }
        val unique = depTracker.reportNewlyEnabled(snd, rcv, msg)
        if (!crosses_partition(snd, rcv)) {
          pendingEvents.insert((uniq, unique))
        }
      }
      case ExternalMessage => {
        if (MessageTypes.fromFailureDetector(msg) ||
            MessageTypes.fromCheckpointCollector(msg)) {
          pendingSystemMessages += uniq
        } else {
          val unique = depTracker.reportNewlyEnabled(snd, rcv, msg)
          pendingEvents.insert(uniq, unique)
        }
      }
      case SystemMessage => None
      case CheckpointReplyMessage =>
        if (checkpointer.done && !blockedOnCheckpoint.get) {
          val violation = test_invariant(trace, checkpointer.checkpoints)
          require(violationFound == None)
          violationFound = violationMatches(violation)
        }
    }

    // Record this MsgSend as a special if it was sent from a timer.
    snd = if (isTimer) "Timer" else snd
    event_orchestrator.events.appendMsgSend(snd, rcv, envelope.message, uniq.id)
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
      prepareCheckpoint()
    }

    // Proceed normally.
    send_external_messages()
    // Always prioritize system messages.
    if (!pendingSystemMessages.isEmpty) {
      val uniq = pendingSystemMessages.dequeue()
      event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
      return Some(uniq.element)
    }

    // Do we have some pending events
    if (pendingEvents.isEmpty) {
      return None
    }

    messagesScheduledSoFar += 1
    if (messagesScheduledSoFar == Int.MaxValue) {
      messagesScheduledSoFar = 1
    }
    val (uniq, unique) = pendingEvents.removeRandomElement()
    event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
    depTracker.reportNewlyDelivered(unique)
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

  def notify_timer_cancel(rcv: ActorRef, msg: Any): Unit = {
    if (handle_timer_cancel(rcv, msg)) {
      return
    }
    // Awkward, we need to walk through the entire hashset to find what we're
    // looking for.
    val toRemove = pendingEvents.arr.find((element) => {
      val otherRcv = element._1._1.element._1.self.path.name
      val otherMsg = element._1._1.element._2.message
      rcv.path.name == otherRcv && msg == otherMsg
    })
    toRemove match {
      case Some(e) =>
        pendingEvents.remove(e)
      case None =>
        // It was already delivered. This is weird that the application is
        // still trying to cancel it, but I don't think it's a violation of
        // soundness on our part.
        None
    }
  }

  override def enqueue_timer(receiver: String, msg: Any) { handle_timer(receiver, msg) }

  override def reset_all_state () {
    // TODO(cs): also reset Instrumenter()'s state?
    reset_state
    // N.B. important to clear our state after we invoke reset_state, since
    // it's possible that enqueue_message may be called during shutdown.
    super.reset_all_state
    pendingEvents = new RandomizedHashSet[Tuple2[Uniq[(ActorCell,Envelope)],Unique]]
    pendingSystemMessages = new Queue[Uniq[(ActorCell, Envelope)]]
    lookingFor = None
    violationFound = None
    trace = null
    messagesScheduledSoFar = 0
    lastCheckpoint = 0
    depTracker = new DepTracker(messageFingerprinter)
    event_orchestrator.setKillCallback(depTracker.reportKill)
    event_orchestrator.setPartitionCallback(depTracker.reportPartition)
    event_orchestrator.setUnPartitionCallback(depTracker.reportUnPartition)
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
