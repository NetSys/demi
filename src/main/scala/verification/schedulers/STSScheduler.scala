package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.immutable.Set
import scala.collection.mutable.Iterable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet
import scala.collection.JavaConversions._

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap
import java.util.Collections

import scala.util.control.Breaks._

// TODO(cs): MAJOR issue: we need the failure detector to actually respond...
// since some of the ExternalEvents (e.g. Kills) may have been pruned, yet the
// MsgEvents for all originally sent failure detector messages are still in
// the original trace.

// TODO(cs): ALSO: if a Send event is pruned, its MsgEvent will still be
// present...

object STSScheduler {
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
    // Skip over first event,
    val i = events.tail.indexWhere(e =>
      EventTypes.externalEventTypes.contains(e.getClass))
    if (i == -1) {
      return events
    }
    return events.slice(0, i)
  }

  // Filter all external events in original_trace that aren't in subseq
  // TODO(cs): factor this logic out into a generic external events
  // fingerprint comparison?
  def subsequenceIntersection(original_trace: Seq[Event],
                              subseq: Seq[ExternalEvent]) : Seq[Event] = {
    var remaining = ListBuffer[ExternalEvent]() ++ subseq
    var result = ListBuffer[Event]()
    for (event <- original_trace) {
      if (remaining.isEmpty) {
        if (!EventTypes.externalEventTypes.contains(event.getClass)) {
          result += event
        }
      } else {
        event match {
          case KillEvent(actor1) =>
            remaining(0) match {
              case Kill(actor2) => {
                if (actor1 == actor2) {
                  result += event
                  remaining = remaining.tail
                }
              }
              case _ => None
            }
          case PartitionEvent((a1,b1)) =>
            remaining(0) match {
              case Partition(a2,b2) => {
                if (a1 == a2 && b1 == b2) {
                  result += event
                  remaining = remaining.tail
                }
              }
              case _ => None
            }
          case UnPartitionEvent((a1,b1)) =>
            remaining(0) match {
              case UnPartition(a2,b2) => {
                if (a1 == a2 && b1 == b2) {
                  result += event
                  remaining = remaining.tail
                }
              }
              case _ => None
            }
          case SpawnEvent(_,_,name1,_) =>
            remaining(0) match {
              case Start(_, name2) => {
                if (name1 == name2) {
                  result += event
                  remaining = remaining.tail
                }
              }
              case _ => None
            }
          case i: Event => result += i
        }
      }
    }
    return result
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
 *
 * More specifically:
 *   - Use PeekScheduler to infer what internal events are and are not going
 *     to occur for this external event subsequence.
 *   - Then, for each event in original, if it's going to occur, do whatever
 *     it takes (including allowing unexpected events through) to get it to
 *     occur. Else, ignore it.
 */
// Precondition: the first event in original_trace is one of EventTypes.externalEventTypes
class STSScheduler(original_trace: Seq[Event]) extends AbstractScheduler
    with ExternalEventInjector[Event] with TestOracle {
  assume(!original_trace.isEmpty)
  assume(EventTypes.externalEventTypes.contains(original_trace(0).getClass))

  init_failure_detector(enqueue_message)

  var test_invariant : Invariant = null

  // Have we started off the execution yet?
  private[this] var firstMessage = true

  // Current set of enabled events.
  // (snd, rcv, msg) => Queue(rcv's cell, envelope of message)
  val pendingEvents = new HashMap[(String, String, Any), Queue[(ActorCell, Envelope)]]

  // A set of external messages to send. Messages sent between actors are
  // not queued here.
  var messagesToSend = Collections.newSetFromMap(new
          ConcurrentHashMap[(ActorRef, Any),java.lang.Boolean])

  // Return value of PeekScheduler.peek(), which we use to infer whether
  // expected internal events are going to occur or not.
  var peekedEvents : Seq[Event] = null

  // A slice of peekedEvents. Invariant: the first element in
  // currentPeekWindow is always the last external event we injected.
  // The subsequent events in the window are internal events (if any) leading
  // up to but not including the next external event we'll need to inject.
  var currentPeekWindow : ListBuffer[Event] = null

  // Pre: there is a SpawnEvent for every sender and receipient of every SendEvent
  // Pre: subseq is not empty.
  def test (subseq: Seq[ExternalEvent]) : Boolean = {
    assume(!subseq.isEmpty)
    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }

    // Peek() forward to infer which internal events are going to occur.
    // TODO(cs): at this point the actor system has already been started, so
    // I'm not sure this is going to work.
    val peeker = new PeekScheduler
    Instrumenter().scheduler = peeker
    peekedEvents = peeker.peek(subseq)
    assume(!peekedEvents.isEmpty)
    assume(EventTypes.externalEventTypes.contains(peekedEvents(0).getClass))
    // Just for kicks, see if the Peek run triggered the violation...
    if (test_invariant(subseq)) {
      return true
    }
    peeker.shutdown
    Instrumenter().scheduler = this

    // Now proceed with our execution.
    // We use the original trace as our reference point as we step through the
    // execution.
    val filtered = STSScheduler.subsequenceIntersection(original_trace, subseq)
    event_orchestrator.set_trace(filtered)
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
    return test_invariant(subseq)
  }

  // Invoke this whenever we have just injected an external event. Slides
  // peekEvents forward up past that external event so our "peek window"
  // always starts at the next external event we're going to inject.
  def slidePeekWindow(e: Event) {
    assume(EventTypes.externalEventTypes.contains(e.getClass))
    currentPeekWindow = ListBuffer() ++ STSScheduler.getNextInterval(peekedEvents)
    assume(!currentPeekWindow.isEmpty)
    assume(e.getClass == currentPeekWindow(0).getClass)
    peekedEvents.drop(currentPeekWindow.length)
  }

  def containedInPeekWindow(m: MsgEvent) : Boolean = {
    val found = currentPeekWindow.find(e =>
      e match {
        case MsgEvent(sender, receiver, msg) =>
          m.sender == sender && m.receiver == receiver && m.msg == msg
        case _ => false
      }
    )

    return found != None
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
            slidePeekWindow(event_orchestrator.current_event)
          case KillEvent (name) =>
            event_orchestrator.trigger_kill(name)
            slidePeekWindow(event_orchestrator.current_event)
          case PartitionEvent((a,b)) =>
            event_orchestrator.trigger_partition(a,b)
            slidePeekWindow(event_orchestrator.current_event)
          case UnPartitionEvent((a,b)) =>
            event_orchestrator.trigger_unpartition(a,b)
            slidePeekWindow(event_orchestrator.current_event)
          // MsgSend is the initial send
          case MsgSend (sender, receiver, message) =>
            // sender == "deadLetters" means the message is external.
            // TODO(cs): don't enqueue this if it's from the failure
            // detector...
            if (sender == "deadLetters") {
              enqueue_message(receiver, message)
              slidePeekWindow(event_orchestrator.current_event)
            }
          // MsgEvent is the delivery
          case m: MsgEvent =>
            // Check if the event is expected to occur
            if (containedInPeekWindow(m)) {
              currentPeekWindow -= m
              break
            }
            // Else ignore this message!
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

  // TODO(cs): redundant with ReplayScheduler. Not sure it's worth factoring
  // out though.
  def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    // TODO(cs): the following comment is broken...
    // TODO(cs): also, need to ensure that ExternalEvents don't end up in
    // PendingEvents?
    // N.B. we do not actually route messages destined for the
    // FailureDetector, we simply take note of them. This is because all
    // responses from the FailureDetector (from the previous execution)
    // are already recorded as external
    // messages, and will be injected by advanceReplay().

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
    // First get us to kind of a good place: it should be the case after
    // invoking advanceReplay() that the next event is a MsgEvent -- an
    // internal message that we observed in both the original execution and
    // the Peek() run.
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

      val nextMessage = expectedMessage match {
        case Some(msgEvent) =>
          // We have found the message we expect!
          event_orchestrator.trace_advanced
          expectedMessage
        case None =>
          // We need to wait awhile, trying some number of unexpected messages
          // and hoping that eventually our expectedMessage will show up.
          // Here, we just find the first unexpected message -- that is, the
          // first pending message which did not show up in our peek run nor
          // our .
          println("expected message not yet found..." + key)
          val original = STSScheduler.getNextInterval(
            event_orchestrator.trace.slice(event_orchestrator.traceIdx, event_orchestrator.trace.length))
          // XXX
          // If there are no unexpected messages, fail by returning None.
          None
      }

      schedSemaphore.release
      nextMessage
    }
  }

  override def notify_quiescence () {
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

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }
}
