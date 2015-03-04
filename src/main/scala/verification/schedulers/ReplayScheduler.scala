package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.actor.FSM.Timer

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.immutable.Set
import scala.collection.mutable.Iterable
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.Breaks._

/**
 * Scheduler that takes a list of both internal and external events (e.g. the
 * return value of PeekScheduler.peek()), and attempts to replay that schedule
 * exactly.
 *
 * In the case of non-determinism we die. In particular:
 *  - If the application sends unexpected messages
 *  - If the application does not send a message that was previously sent
 */
class ReplayScheduler(messageFingerprinter: MessageFingerprinter, enableFailureDetector:Boolean, strictChecking:Boolean)
    extends AbstractScheduler with ExternalEventInjector[Event] with HistoricalScheduler {
  def this() = this(new BasicFingerprinter, false, false)

  if (!enableFailureDetector) {
    disableFailureDetector()
  }

  // Have we started off the execution yet?
  private[this] var firstMessage = true

  // Current set of enabled events.
  // (snd, rcv, msg fingerprint) => Queue(rcv's cell, envelope of message)
  val pendingEvents = new HashMap[(String, String, MessageFingerprint),
                                  Queue[Uniq[(ActorCell, Envelope)]]]

  // Just do a cheap test to ensure that no new unexpected messages are sent. This
  // is not perfect.
  private[this] val allSends = new HashMap[(String, String, MessageFingerprint), Int]

  // Given an event trace, try to replay it exactly. Return the events
  // observed in *this* execution, which should in theory be the same as the
  // original.
  // Pre: there is a SpawnEvent for every sender and receipient of every SendEvent
  def replay (_trace: EventTrace) : EventTrace = {
    if (!(Instrumenter().scheduler eq this)) {
      throw new IllegalStateException("Instrumenter().scheduler not set!")
    }
    // We don't actually want to allow the failure detector to send messages,
    // since all the failure detector messages are recorded in _trace. So we
    // give it a no-op enqueue_message parameter.
    if (enableFailureDetector) {
      fd = new FDMessageOrchestrator((s: String, m: Any) => Unit)
      event_orchestrator.set_failure_detector(fd)
      fd.startFD(instrumenter.actorSystem)
    }

    if (!alreadyPopulated) {
      populateActorSystem(_trace.getEvents flatMap {
        case SpawnEvent(_,props,name,_) => Some((props, name))
        case _ => None
      })
    }

    for (t <- _trace.getEvents) {
      t match {
        case MsgSend (snd, rcv, msg) =>
          // Track all messages we expect.
          val fingerprint = messageFingerprinter.fingerprint(msg)
          allSends((snd, rcv, fingerprint)) = allSends.getOrElse((snd, rcv, fingerprint), 0) + 1
        case _ =>
          None
      }
    }

    val updatedEvents = updateEvents(_trace.getEvents)
    event_orchestrator.set_trace(updatedEvents)
    // Bad method name. "reset recorded events"
    event_orchestrator.reset_events

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
              if (Instrumenter().timerToCancellable contains ((receiver, message))) {
                Instrumenter().manuallyHandleTick(receiver, message)
              } else {
                enqueue_message(receiver, message)
              }
            }
          case TimerSend(fingerprint) =>
            if (scheduledFSMTimers contains fingerprint) {
              val timer = scheduledFSMTimers(fingerprint)
              Instrumenter().manuallyHandleTick(fingerprint.receiver, timer)
            } else {
              throw new RuntimeException("Expected TimerSend("+fingerprint+")")
            }
          case TimerDelivery(fingerprint) =>
            break
          case MsgEvent(snd, rcv, msg) =>
            break
          case BeginWaitQuiescence =>
            event_orchestrator.events += BeginWaitQuiescence
            //event_orchestrator.trace_advanced
            //break
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
    val fingerprint = messageFingerprinter.fingerprint(msg)
    // N.B. we do not actually route messages destined for the
    // FailureDetector, we simply take note of them. This is because all
    // responses from the FailureDetector (from the previous execution)
    // are already recorded as external
    // messages, and will be injected by advanceReplay().

    if (strictChecking) {
      if (!allSends.contains((snd, rcv, fingerprint)) ||
           allSends((snd, rcv, fingerprint)) <= 0) {
        throw new RuntimeException("Unexpected message " + (snd, rcv, msg))
      }
      allSends((snd, rcv, fingerprint)) = allSends.getOrElse((snd, rcv, fingerprint), 0) - 1
    }

    val uniq = Uniq[(ActorCell, Envelope)]((cell, envelope))
    event_orchestrator.events.appendMsgSend(snd, rcv, envelope.message, uniq.id)
    // Drop any messages that crosses a partition.
    if (!event_orchestrator.crosses_partition(snd, rcv) && rcv != FailureDetector.fdName) {
      val msgs = pendingEvents.getOrElse((snd, rcv, fingerprint),
                          new Queue[Uniq[(ActorCell, Envelope)]])
      pendingEvents((snd, rcv, fingerprint)) = msgs += uniq
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
    // First get us to kind of a good place: it should be the case after
    // invoking advanceReplay() that the next event is a MsgEvent or
    // TimerDelivery event.
    advanceReplay()
    // Make sure to send any external messages that just got enqueued
    send_external_messages()

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
          (snd, rcv, messageFingerprinter.fingerprint(msg))
        case TimerDelivery(timer_fingerprint) =>
          val timer = scheduledFSMTimers(timer_fingerprint)
          (timer_fingerprint.sender, timer_fingerprint.receiver,
           messageFingerprinter.fingerprint(timer))
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

      nextMessage match {
        case None =>
          println("pending keys:")
          for (pending <- pendingEvents.keys) {
            println(pending)
          }
          throw new RuntimeException("Expected event " + key)
        case Some(uniq) => None
          if (!(event_orchestrator.actorToActorRef.values contains uniq.element._1.self)) {
            throw new IllegalStateException("unknown ActorRef: " + uniq.element._1.self)
          }
          event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
          schedSemaphore.release
          return Some(uniq.element)
      }
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

  override def notify_timer_scheduled(sender: ActorRef, receiver: ActorRef,
                                      msg: Any): Boolean = {
    handle_timer_scheduled(sender, receiver, msg, messageFingerprinter)
    return false
  }
}
