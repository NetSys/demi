package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean


/**
 * Takes a sequence of ExternalEvents as input, and plays the execution
 * forward in the same way as FairScheduler. While playing forward,
 * PeekScheduler records all internal events that occur, e.g. Message Sends.
 * PeekScheduler finally returns all events observed during the execution, including
 * external and internal events.
 */
class PeekScheduler()
    extends FairScheduler with ExternalEventInjector with TestOracle {

  var test_invariant : Invariant = null

  def peek (_trace: Seq[ExternalEvent]) : Queue[Event]  = {
    return execute_trace(_trace)
  }

  override def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])
    handle_event_produced(snd, rcv, envelope) match {
      case SystemMessage => None
      case _ => {
        if (!crosses_partition(snd, rcv)) {
          pendingEvents(rcv) = msgs += ((cell, envelope))
        }
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
    enqueue_external_messages
    // FairScheduler gives us round-robin message dispatches.
    return super.schedule_new_message()
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

  override def after_receive(cell: ActorCell) : Unit = {
    handle_after_receive(cell)
  }

  override def enqueue_message(receiver: String, msg: Any) {
    super[ExternalEventInjector].enqueue_message(receiver, msg)
  }

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }

  def test(events: Seq[ExternalEvent]) : Boolean = {
    Instrumenter().scheduler = this
    peek(events)
    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }
    val passes = test_invariant(events)
    shutdown()
    return passes
  }
}
