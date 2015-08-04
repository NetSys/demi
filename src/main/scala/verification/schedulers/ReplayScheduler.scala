package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Cell, ActorRef, ActorSystem, Props}

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

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

class ReplayException(message:String=null, cause:Throwable=null) extends
      RuntimeException(message, cause)

/**
 * Scheduler that takes a list of both internal and external events (e.g. the
 * return value of PeekScheduler.peek()), and attempts to replay that schedule
 * exactly.
 *
 * In the case of non-determinism we die. In particular:
 *  - If the application sends unexpected messages
 *  - If the application does not send a message that was previously sent
 */
class ReplayScheduler(val schedulerConfig: SchedulerConfig,
                      strictChecking:Boolean=false)
    extends AbstractScheduler with ExternalEventInjector[Event] {

  val messageFingerprinter = schedulerConfig.messageFingerprinter

  val logger = LoggerFactory.getLogger("ReplaySched")

  // Have we started off the execution yet?
  private[this] var firstMessage = true

  // Current set of enabled events.
  // (snd, rcv, msg fingerprint) => Queue(rcv's cell, envelope of message)
  val pendingEvents = new HashMap[(String, String, MessageFingerprint),
                                  Queue[Uniq[(Cell, Envelope)]]]

  // Current set of failure detector or CheckpointRequest messages destined for
  // actors, to be delivered in the order they arrive.
  // Always prioritized over internal messages.
  var pendingSystemMessages = new Queue[(Cell, Envelope)]

  // Just do a cheap test to ensure that no new unexpected messages are sent. This
  // is not perfect.
  private[this] val allSends = new HashMap[(String, String, MessageFingerprint), Int]

  // If != "", there was non-determinism. Have the main thread throw an
  // exception, so that it can be caught by the caller.
  private[this] var nonDeterministicErrorMsg = ""

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
    if (schedulerConfig.enableFailureDetector) {
      fd = new FDMessageOrchestrator((o: Option[ActorRef], s: String, m: Any) => Unit)
      event_orchestrator.set_failure_detector(fd)
      fd.startFD(instrumenter.actorSystem)
    }
    if (schedulerConfig.enableCheckpointing) {
      checkpointer.startCheckpointCollector(Instrumenter().actorSystem)
    }

    if (!alreadyPopulated) {
      if (actorNamePropPairs != null) {
        populateActorSystem(actorNamePropPairs)
      } else {
        populateActorSystem(_trace.getEvents flatMap {
          case SpawnEvent(_,props,name,_) => Some((props, name))
          case _ => None
        })
      }
    }

    for (t <- _trace.getEvents) {
      t match {
        case MsgSend (snd, rcv, msg) =>
          // Track all messages we expect.
          val fingerprint = messageFingerprinter.fingerprint(msg)
          val correctedSnd = if (snd == "Timer") "deadLetters" else snd
          allSends((correctedSnd, rcv, fingerprint)) = allSends.getOrElse((correctedSnd, rcv, fingerprint), 0) + 1
        case _ =>
          None
      }
    }
    val updatedEvents = _trace.recomputeExternalMsgSends(_trace.original_externals)
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
    if (nonDeterministicErrorMsg != "") {
      throw new ReplayException(message=nonDeterministicErrorMsg)
    }
    schedulerConfig.invariant_check match {
      case Some(check) =>
        val checkpoint = takeCheckpoint()
        val violation = check(List.empty, checkpoint)
        // TODO(cs): actually return this rather than printing it
        println("Violation?: " + violation)
      case None =>
    }
    event_orchestrator.events.setOriginalExternalEvents(_trace.original_externals)
    return event_orchestrator.events
  }

  def advanceReplay() {
    schedSemaphore.acquire
    started.set(true)
    var loop = true
    breakable {
      while (loop && !event_orchestrator.trace_finished) {
        // println("Replaying " + event_orchestrator.traceIdx + "/" +
        //   event_orchestrator.trace.length + " " + event_orchestrator.current_event)
        event_orchestrator.current_event match {
          case SpawnEvent (_, _, name, _) =>
            event_orchestrator.trigger_start(name)
          case KillEvent (name) =>
            event_orchestrator.trigger_kill(name)
          case PartitionEvent((a,b)) =>
            event_orchestrator.trigger_partition(a,b)
          case UnPartitionEvent((a,b)) =>
            event_orchestrator.trigger_unpartition(a,b)
          case m @ MsgSend (sender, receiver, message) =>
            if (EventTypes.isExternal(m)) {
              enqueue_message(None, receiver, message)
            }
          case t: TimerDelivery =>
            // Check that the Timer wasn't destined for a dead actor.
            send_external_messages(false)
            if (pendingEvents contains (t.sender, t.receiver, t.fingerprint)) {
              break
            }
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
  def event_produced(cell: Cell, envelope: Envelope) : Unit = {
    var snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message

    if (MessageTypes.fromCheckpointCollector(msg)) {
      pendingSystemMessages += ((cell, envelope))
      return
    }

    if (rcv == CheckpointSink.name && schedulerConfig.enableCheckpointing) {
      checkpointer.handleCheckpointResponse(envelope.message, snd)
      return
    }

    val fingerprint = messageFingerprinter.fingerprint(msg)
    // N.B. we do not actually route messages destined for the
    // FailureDetector, we simply take note of them. This is because all
    // responses from the FailureDetector (from the previous execution)
    // are already recorded as external
    // messages, and will be injected by advanceReplay().

    if (strictChecking) {
      if (!allSends.contains((snd, rcv, fingerprint)) ||
           allSends((snd, rcv, fingerprint)) <= 0) {
        // Have the main thread crash on our behalf (once Quiescence is
        // reached)
        nonDeterministicErrorMsg = "Unexpected message " + (snd, rcv, msg)
      }
      allSends((snd, rcv, fingerprint)) = allSends.getOrElse((snd, rcv, fingerprint), 0) - 1
    }

    val uniq = Uniq[(Cell, Envelope)]((cell, envelope))
    // Drop any messages that crosses a partition.
    if (!event_orchestrator.crosses_partition(snd, rcv) && rcv != FailureDetector.fdName) {
      val msgs = pendingEvents.getOrElse((snd, rcv, fingerprint),
                          new Queue[Uniq[(Cell, Envelope)]])
      pendingEvents((snd, rcv, fingerprint)) = msgs += uniq
    }

    // Record this MsgSend as a special if it was sent from a timer.
    val isTimer = (snd == "deadLetters" &&
                   !enqueuedExternalMessages.contains(msg))
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

  // TODO: The first message send ever is not queued, and hence leads to a bug.
  // Solve this someway nice.
  // TODO(cs): make sure not to send to blockedActors! Then again, that would
  // indicate divergence, so maybe no need.
  def schedule_new_message(blockedActors: Set[String]) : Option[(Cell, Envelope)] = {
    if (nonDeterministicErrorMsg != "") {
      return None
    }

    // First get us to kind of a good place: it should be the case after
    // invoking advanceReplay() that the next event is a MsgEvent or
    // TimerDelivery event.
    advanceReplay()
    // Make sure to send any external messages that just got enqueued
    send_external_messages()

    if (!pendingSystemMessages.isEmpty) {

      return Some(pendingSystemMessages.dequeue)
    }

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
        case t: TimerDelivery =>
          (t.sender, t.receiver, t.fingerprint)
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
          // Have the main thread crash on our behalf
          nonDeterministicErrorMsg = "Expected event " + key
          schedSemaphore.release
          return None
        case Some(uniq) => None
          if (!(event_orchestrator.actorToActorRef.values contains uniq.element._1.self)) {
            throw new IllegalStateException("unknown ActorRef: " + uniq.element._1.self)
          }

          if (logger.isTraceEnabled()) {
            val cell = uniq.element._1
            val envelope = uniq.element._2
            val snd = envelope.sender.path.name
            val rcv = cell.self.path.name
            val msg = envelope.message
            logger.trace("schedule_new_message(): " + snd + " -> " + rcv + " " + msg)
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

    if (blockedOnCheckpoint.get()) {
      checkpointSem.release()
      return
    }
    if (nonDeterministicErrorMsg != "") {
      traceSem.release
      return
    }
    if (!event_orchestrator.trace_finished) {
      // Have the main thread crash on our behalf
      nonDeterministicErrorMsg = "Divergence"
      traceSem.release
    } else {
      if (currentlyInjecting.get) {
        // Tell the calling thread we are done
        traceSem.release
      } else {
        throw new RuntimeException("currentlyInjecting.get returned false")
      }
    }
  }

  def notify_timer_cancel(rcv: String, msg: Any): Unit = {
    if (handle_timer_cancel(rcv, msg)) {
      return
    }
    val key = ("deadLetters", rcv, messageFingerprinter.fingerprint(msg))
    pendingEvents.get(key) match {
      case Some(queue) =>
        queue.dequeueFirst(t => t.element._2.message == msg)
        if (queue.isEmpty) {
          pendingEvents.remove(key)
        }
      case None => None
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
  override def before_receive(cell: Cell) {
    super.before_receive(cell)
    handle_before_receive(cell)
  }

  // Called after receive is done being processed
  override def after_receive(cell: Cell) {
    handle_after_receive(cell)
  }

  override def enqueue_timer(receiver: String, msg: Any) { handle_timer(receiver, msg) }
}
