package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.immutable.Set
import scala.collection.mutable.Iterable
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap
import java.util.Collections

import scala.util.control.Breaks._

/**
 * Scheduler that takes a list of both internal and external events (e.g. the
 * return value of PeekScheduler.peek()), and attempts to replay that schedule
 * exactly.
 *
 * Deals with non-determinism as follows:
 *  - If the application sends unexpected messages, this scheduler does not allow them through.
 *  - If the application does not send a message that was previously sent,
 *    die.
 */
class ReplayScheduler()
    extends AbstractScheduler with ExternalEventInjector[Event] {

  init_failure_detector(enqueue_message)

  // Have we started off the execution yet?
  private[this] var firstMessage = true

  // Current set of enabled events.
  val pendingEvents = new HashMap[(String, String, Any), Queue[(ActorCell, Envelope)]]

  // A set of external messages to send. Messages sent between actors are
  // not queued here.
  var messagesToSend = Collections.newSetFromMap(new
          ConcurrentHashMap[(ActorRef, Any),java.lang.Boolean])

  // Just do a cheap test to ensure that no new unexpected messages are sent. This
  // is not perfect.
  private[this] val allSends = new HashMap[(String, String, Any), Int]

  // Given an external event trace, see the events produced
  // Pre: there is a SpawnEvent for every sender and receipient of every SendEvent
  def replay (_trace: Seq[Event]) : Queue[Event] = {
    event_orchestrator.set_trace(_trace)
    fd.startFD(instrumenter.actorSystem)
    // We begin by starting all actors at the beginning of time, just mark them as
    // isolated (i.e., unreachable)
    for (t <- event_orchestrator.trace) {
      t match {
        case SpawnEvent (_, props, name, _) =>
          // Just start and isolate all actors we might eventually care about
          instrumenter.actorSystem.actorOf(props, name)
          event_orchestrator.isolate_node(name)
        case MsgSend (snd, rcv, msg) =>
          // Track all messages we expect.
          allSends((snd, rcv, msg)) = allSends.getOrElse((snd, rcv, msg), 0) + 1
        case _ =>
          None
      }
    }
    currentlyInjecting.set(true)
    // Start playing back trace
    advanceReplay()
    // Have this thread wait until the trace is down. This allows us to safely notify
    // the caller.
    traceSem.acquire
    currentlyInjecting.set(false)
    return event_orchestrator.events
  }

  def advanceReplay() {
    schedSemaphore.acquire
    started.set(true)
    var loop = true
    breakable {
      while (loop && !event_orchestrator.trace_finished) {
        event_orchestrator.current_event match {
          case SpawnEvent (_, _, name, _) =>
            event_orchestrator.trigger_start(name)
          case KillEvent (name) =>
            event_orchestrator.trigger_kill(name)
          case PartitionEvent((a,b)) =>
            event_orchestrator.trigger_partition(a,b)
          case UnPartitionEvent((a,b)) =>
            event_orchestrator.trigger_unpartition(a,b)
          case MsgSend (sender, receiver, message) =>
            // sender == "deadLetters" means the message is external.
            if (sender == "deadLetters") {
              enqueue_message(receiver, message)
            }
          case MsgEvent(snd, rcv, msg) =>
            break
          case Quiescence =>
            // This is just a nop. Do nothing
            event_orchestrator.events += Quiescence
          case ChangeContext(_) => () // Check what is going on
        }
        event_orchestrator.trace_advanced
      }
    }
    schedSemaphore.release
    // OK this is the first time round, let us start dispatching
    if (firstMessage) {
      firstMessage = false
      instrumenter.start_dispatch()
    }
  }

  // Check no unexpected messages are enqueued
  def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    // N.B. we do not actually route messages destined for the
    // FailureDetector, we simply take note of them. This is because all
    // responses from the FailureDetector (from the previous execution)
    // are already recorded as external
    // messages, and will be injected by advanceReplay().

    if (!allSends.contains((snd, rcv, msg)) ||
         allSends((snd, rcv, msg)) <= 0) {
      throw new RuntimeException("Unexpected message " + (snd, rcv, msg))
    }
    allSends((snd, rcv, msg)) = allSends.getOrElse((snd, rcv, msg), 0) - 1
    event_orchestrator.events += MsgSend(snd, rcv, envelope.message)
    // Drop any messages that crosses a partition.
    if (!event_orchestrator.crosses_partition(snd, rcv)) {
      val msgs = pendingEvents.getOrElse((snd, rcv, msg),
                          new Queue[(ActorCell, Envelope)])
      pendingEvents((snd, rcv, msg)) = msgs += ((cell, envelope))
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

  // TODO: The first message send ever is not queued, and hence leads to a bug.
  // Solve this someway nice.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    // First get us to kind of a good place
    advanceReplay()
    // Make sure to send any external messages that just got enqueued
    enqueue_external_messages(messagesToSend)

    if (event_orchestrator.trace_finished) {
      // We are done, let us wait for notify_quiescence to notice this
      // FIXME: We could check here to see if there are any pending messages.
      None
    } else {
      // Ensure that only one thread is accessing shared scheduler structures
      schedSemaphore.acquire

      // Pick next message based on trace. We are guaranteed to be at a MsgEvent
      // unless something else went wrong.
      val key = event_orchestrator.current_event match {
        case MsgEvent(snd, rcv, msg) =>
          (snd, rcv, msg)
        case _ =>
         throw new Exception("Replay error")
      }
      // Now that we have found the key advance to the next thing to replay
      event_orchestrator.trace_advanced
      val nextMessage = pendingEvents.get(key) match {
        case Some(queue) =>
          if (queue.isEmpty) {
            println("queue.isEmpty")
            // Message not enabled
            pendingEvents.remove(key)
            None
          } else {
            val willRet = queue.dequeue()
            if (queue.isEmpty) {
              pendingEvents.remove(key)
            }
            Some(willRet)
          }
        case None =>
          println("key not found")
          // Message not enabled
          None
      }

      // N.B. it is an error if nextMessage is None, since it means that a
      // message we expected to occur did not show up. If this occurs, we are
      // going to crash, but not quite yet. After this method returns,
      // notify_quiescence will be immediately called, and we will crash there
      // since since we are not yet at the end of the trace.
      nextMessage match {
        case None =>
          println("Error: expected event " + key)
        case _ => None
      }

      schedSemaphore.release
      nextMessage
    }
  }

  override def notify_quiescence () {
    assert(started.get)
    started.set(false)
    if (!event_orchestrator.trace_finished) {
      // If waiting for quiescence.
      throw new Exception("Divergence")
    } else {
      if (currentlyInjecting.get) {
        // Tell the calling thread we are done
        traceSem.release
      } else {
        throw new RuntimeException("currentlyInjecting.get returned false")
      }
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

  // Called before we start processing a newly received event
  override def before_receive(cell: ActorCell) {
    super.before_receive(cell)
    handle_before_receive(cell)
  }

  // Called after receive is done being processed
  override def after_receive(cell: ActorCell) {
    handle_after_receive(cell)
  }

  // Enqueue an external message for future delivery
  override def enqueue_message(receiver: String, msg: Any) {
    if (event_orchestrator.actorToActorRef contains receiver) {
      enqueue_message(event_orchestrator.actorToActorRef(receiver), msg)
    } else {
      throw new IllegalArgumentException("Unknown receiver " + receiver)
    }
  }

  def enqueue_message(actor: ActorRef, msg: Any) {
    handle_enqueue_message(actor, msg)
    messagesToSend += ((actor, msg))
  }
}
