package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, Cell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

// TODO(cs): PeekScheduler should really be parameterized to allow us to try
// different scheduling strategies (FIFO, round-robin) during Peek.

/**
 * Takes a sequence of ExternalEvents as input, and plays the execution
 * forward in the same way as FairScheduler. While playing forward,
 * PeekScheduler records all internal events that occur, e.g. Message Sends.
 * PeekScheduler finally returns all events observed during the execution, including
 * external and internal events.
 */
class PeekScheduler(enableFailureDetector: Boolean)
    extends FairScheduler with ExternalEventInjector[ExternalEvent] with TestOracle {
  def this() = this(true)

  def getName: String = "FairScheduler"

  // Allow the user to place a bound on how many messages are delivered.
  // Useful for dealing with non-terminating systems.
  var maxMessages = Int.MaxValue
  def setMaxMessages(_maxMessages: Int) = {
    maxMessages = _maxMessages
  }

  var messagesScheduledSoFar = 0

  var test_invariant : Invariant = null

  enableCheckpointing()

  if (!enableFailureDetector) {
    disableFailureDetector()
  }

  def peek (_trace: Seq[ExternalEvent]) : EventTrace = {
    if (!(Instrumenter().scheduler eq this)) {
      throw new IllegalStateException("Instrumenter().scheduler not set!")
    }
    event_orchestrator.events.setOriginalExternalEvents(_trace)
    return execute_trace(_trace)
  }

  override def event_produced(cell: Cell, envelope: Envelope) = {
    var snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[Uniq[(Cell, Envelope)]])
    val uniq = Uniq[(Cell, Envelope)]((cell, envelope))
    var isTimer = false
    handle_event_produced(snd, rcv, envelope) match {
      case FailureDetectorQuery => None
      case CheckpointReplyMessage => None
      case ExternalMessage => None
      case InternalMessage => {
        if (snd == "deadLetters") {
          isTimer = true
        }
        if (!crosses_partition(snd, rcv)) {
          pendingEvents(rcv) = msgs += uniq
        }
      }
    }
    // Record this MsgEvent as a special if it was sent from a timer.
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
  override def event_consumed(cell: Cell, envelope: Envelope) = {
    handle_event_consumed(cell, envelope)
  }

  // TODO(cs): make sure not to send to blockedActors!
  override def schedule_new_message(blockedActors: Set[String]) : Option[(Cell, Envelope)] = {
    // Check if we've exceeded our message limit
    if (messagesScheduledSoFar > maxMessages) {
      println("Exceeded maxMessages")
      event_orchestrator.finish_early
      return None
    }

    send_external_messages()
    // FairScheduler gives us round-robin message dispatches.
    val uniq_option = find_message_to_schedule(blockedActors)
    uniq_option match {
      case Some(uniq) =>
        event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
        uniq.element._2.message match {
          case CheckpointRequest => None
          case _ =>
            messagesScheduledSoFar += 1
            if (messagesScheduledSoFar == Int.MaxValue) {
              messagesScheduledSoFar = 1
            }
        }
        return Some(uniq.element)
      case None =>
        return None
    }
  }

  override def notify_quiescence () {
    handle_quiescence
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

  override def after_receive(cell: Cell) : Unit = {
    handle_after_receive(cell)
  }

  override def before_receive(cell: Cell) : Unit = {
    handle_before_receive(cell)
  }

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }

  override def enqueue_message(sender: Option[ActorRef], receiver: String, msg: Any) = {
    super[ExternalEventInjector].enqueue_message(sender, receiver, msg)
  }

  override def notify_timer_cancel(receiver: ActorRef, msg: Any): Unit = {
    if (handle_timer_cancel(receiver, msg)) {
      return
    }
    super.notify_timer_cancel(receiver, msg)
  }

  override def enqueue_timer(receiver: String, msg: Any) { handle_timer(receiver, msg) }

  override def reset_all_state() = {
    super.reset_all_state
    messagesScheduledSoFar = 0
  }

  def test(events: Seq[ExternalEvent],
           violation_fingerprint: ViolationFingerprint,
           stats: MinimizationStats,
           init:Option[()=>Any]=None) : Option[EventTrace] = {
    Instrumenter().scheduler = this
    peek(events)
    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }
    val checkpoint = takeCheckpoint()
    val violation = test_invariant(events, checkpoint)
    var violation_found = false
    violation match {
      case Some(fingerprint) =>
        violation_found = fingerprint.matches(violation_fingerprint)
      case None => None
    }
    val ret = violation_found match {
      case true => Some(event_orchestrator.events)
      case false => None
    }

    // reset ExternalEventInjector
    reset_state(true)
    // reset FairScheduler
    reset_all_state

    return ret
  }
}
