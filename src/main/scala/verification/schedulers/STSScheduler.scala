package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.immutable.Set
import scala.collection.mutable.Iterable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.Breaks._

// TODO(cs): if a Start event is pruned, we should prune all MsgSend and
// MsgEvents for that actor, at least for the "Start"/"Kill" interval.

// TODO(cs): implement fingerprints for MsgEvent.message (domain-specific) types.

object STSScheduler {
  // The maximum number of unexpected messages to try in a Peek() run before
  // giving up.
  val maxPeekMessagesToTry = 100

  /**
   * Return a slice of events:
   *   events[0...index of first external event that is not events(0)]
   *
   * Or an empty list if events is empty.
   * Or events if there are no more external events.
   *
   * ... is exclusive
   */
  def getNextInterval(events: Seq[Event]) : Seq[Event] = {
    if (events.isEmpty) {
      return events
    }
    // Skip over first event.
    val i = events.tail.indexWhere(e => EventTypes.isExternal(e))
    if (i == -1) {
      return events
    }
    return events.slice(0, i)
  }
}

/**
 * Scheduler that takes an execution history (e.g. the
 * return value of RandomScheduler.peek()), as well as a subsequence of
 * ExternalEvents. Attempts to find a schedule containing the ExternalEvents
 * that triggers a given invariant.
 *
 * Follows essentially the same heuristics as STS1:
 *   http://www.eecs.berkeley.edu/~rcs/research/sts.pdf
 */
class STSScheduler(var original_trace: EventTrace, allowPeek: Boolean) extends AbstractScheduler
    with ExternalEventInjector[Event] with TestOracle {
  assume(!original_trace.isEmpty)

  def this(original_trace: EventTrace) = this(original_trace, false)

  var test_invariant : Invariant = null

  // Have we started off the execution yet?
  private[this] var firstMessage = true

  // Current set of enabled events. Includes external messages, but not
  // failure detector messages, which are always sent in FIFO order.
  // (snd, rcv, msg) => Queue(rcv's cell, envelope of message)
  val pendingEvents = new HashMap[(String, String, Any),
                                  Queue[Uniq[(ActorCell, Envelope)]]]

  // Current set of failure detector messages destined for actors, to be delivered
  // in the order they arrive. Always prioritized over internal messages.
  var pendingFDMessages = new Queue[Uniq[(ActorCell, Envelope)]]

  // Are we currently pausing the ActorSystem in order to invoke Peek()
  var pausing = new AtomicBoolean(false)

  // Pre: there is a SpawnEvent for every sender and recipient of every SendEvent
  // Pre: subseq is not empty.
  def test (subseq: Seq[ExternalEvent]) : Boolean = {
    assume(!subseq.isEmpty)
    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }

    // We use the original trace as our reference point as we step through the
    // execution.
    val filtered = original_trace.filterFailureDetectorMessages.
                                  subsequenceIntersection(subseq)
    event_orchestrator.set_trace(filtered.getEvents)
    fd.startFD(instrumenter.actorSystem)
    // We begin by starting all actors at the beginning of time, just mark them as
    // isolated (i.e., unreachable)
    for (t <- event_orchestrator.trace) {
      t match {
        case SpawnEvent (_, props, name, _) =>
          // Just start and isolate all actors we might eventually care about
          instrumenter.actorSystem.actorOf(props, name)
          event_orchestrator.isolate_node(name)
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
    val invariant_holds = test_invariant(subseq)
    shutdown()
    return invariant_holds
  }

  def peek() = {
    // TODO(cs): add an optimization here: if no unexpected events to
    // scheduler, give up early.
    val m : MsgEvent = event_orchestrator.current_event.asInstanceOf[MsgEvent]
    // If it isn't enabled yet, let's try Peek()'ing for it.
    val expected = STSScheduler.getNextInterval(
      event_orchestrator.trace.slice(event_orchestrator.traceIdx, event_orchestrator.trace.length))
    val peeker = new IntervalPeekScheduler()
    Instrumenter().scheduler = peeker
    val checkpoint = Instrumenter().checkpoint()
    println("Peek()'ing")
    val prefix = peeker.peek(event_orchestrator.events, m, expected)
    peeker.shutdown
    println("Restoring checkpoint")
    Instrumenter().scheduler = this
    Instrumenter().restoreCheckpoint(checkpoint)
    prefix match {
      case Some(lst) =>
        // Prepend the prefix onto expected events so that
        // schedule_new_message() correctly schedules the prefix.
        event_orchestrator.prepend(lst)
      case None =>
        println("Ignoring message" + m)
        event_orchestrator.trace_advanced
    }

    pausing.set(false)
    // Get us moving again.
    firstMessage = true
    advanceReplay()
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
          // MsgSend is the initial send
          case MsgSend (sender, receiver, message) =>
            // sender == "deadLetters" means the message is external.
            // N.B. we should have pruned all messages from the failure
            // detector -> actors from event_orchestrator.trace, to ensure
            // that we don't send redundant messages.
            if (sender == "deadLetters") {
              enqueue_message(receiver, message)
            }
          // MsgEvent is the delivery
          case m: MsgEvent =>
            // Check if the event is expected to occur
            def messagePending(m: MsgEvent) : Boolean = {
              // Make sure to send any external messages that recently got enqueued
              send_external_messages(false)
              val key = (m.sender, m.receiver, m.msg)
              return pendingEvents.get(key) match {
                case Some(queue) =>
                  !queue.isEmpty
                case None =>
                  false
              }
            }

            val enabled = messagePending(m)
            if (enabled) {
              // Yay, it's already enabled.
              break
            }
            if (allowPeek) {
              // Try peek()'ing for it. First, we need to pause the
              // ActorSystem.
              pausing.set(true)
              break
            }
            // if (!allowPeek) we skip over this event
            println("Ignoring message " + m)
          case Quiescence =>
            // This is just a nop. Do nothing
            event_orchestrator.events += Quiescence
          case BeginWaitQuiescence =>
            event_orchestrator.events += BeginWaitQuiescence
            event_orchestrator.trace_advanced
            break
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

  def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    val uniq = Uniq[(ActorCell, Envelope)]((cell, envelope))
    event_orchestrator.events.appendMsgSend(snd, rcv, envelope.message, uniq.id)
    handle_event_produced(snd, rcv, envelope) match {
      case SystemMessage => None
      case ExternalMessage => {
        // We assume that the failure detector and the outside world always
        // have connectivity with all actors, i.e. no failure detector partitions.
        if (MessageTypes.fromFailureDetector(msg)) {
          pendingFDMessages += uniq
        } else {
          val msgs = pendingEvents.getOrElse((snd, rcv, msg),
                              new Queue[Uniq[(ActorCell, Envelope)]])
          pendingEvents((snd, rcv, msg)) = msgs += uniq
        }
      }
      case InternalMessage => {
        // Drop any messages that crosses a partition.
        if (!event_orchestrator.crosses_partition(snd, rcv)) {
          val msgs = pendingEvents.getOrElse((snd, rcv, msg),
                              new Queue[Uniq[(ActorCell, Envelope)]])
          pendingEvents((snd, rcv, msg)) = msgs += uniq
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

  // TODO: The first message send ever is not queued, and hence leads to a bug.
  // Solve this someway nice.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    if (pausing.get) {
      // Return None to stop dispatching.
      return None
    }

    // Flush detector messages before proceeding with other messages.
    send_external_messages()
    if (!pendingFDMessages.isEmpty) {
      val uniq = pendingFDMessages.dequeue()
      event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
      return Some(uniq.element)
    }

    // OK, now we first need to get to a good place: it should be the case after
    // invoking advanceReplay() that the next event is a MsgEvent -- an
    // internal message that we observed in both the original execution and
    // the Peek() run.
    advanceReplay()

    if (pausing.get) {
      // Return None to stop dispatching.
      return None
    }

    // Make sure to send any external messages that just got enqueued
    send_external_messages()

    // First check if we're finished
    if (event_orchestrator.trace_finished) {
      // We are done, let us wait for notify_quiescence to notice this
      // FIXME: We could check here to see if there are any pending messages.
      return None
    }

    // Otherwise, we're chasing a message we (advanceReplay) knows to be enabled.
    // Ensure that only one thread is accessing shared scheduler structures
    schedSemaphore.acquire

    // Pick next message based on trace.
    val key = event_orchestrator.current_event match {
      case MsgEvent(snd, rcv, msg) =>
        (snd, rcv, msg)
      case _ =>
        // We've broken out of advanceReplay() because of a
        // BeginWaitQuiescence event, but there were no pendingFDMessages to
        // send. So, we need to invoke advanceReplay() once again to get us up
        // to a pending MsgEvent.
        schedSemaphore.release
        return schedule_new_message()
    }

    // Both check if expected message exists, and dequeue() it if it does.
    val expectedMessage = pendingEvents.get(key) match {
      case Some(queue) =>
        if (queue.isEmpty) {
          // Should never really happen..
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
        // Message not enabled
        None
    }

    // Advance the trace
    // It is a fatal error if expectedMessage is None; advanceReplay should
    // have inferred that the next expected message is going to be enabled.
    expectedMessage match {
      case Some(uniq) =>
        // We have found the message we expect!
        event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
        event_orchestrator.trace_advanced
        schedSemaphore.release
        return Some(uniq.element)
      case None =>
        throw new RuntimeException("We expected " + key + " to be enabled..")
    }
  }

  override def notify_quiescence () {
    if (pausing.get) {
      // We've now paused the ActorSystem. Go ahead with peek()
      peek()
      return
    }
    assert(started.get)
    started.set(false)
    if (!event_orchestrator.trace_finished) {
      throw new Exception("Failed to find messages to send to finish the trace!")
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
    println("start_trace")
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

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }
}
