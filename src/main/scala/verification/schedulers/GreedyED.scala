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
import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.Breaks._

// TODO(cs): make EventTrace immutable so that copying is far more efficient.
// TODO(cs): I'm fairly sure this code is full of race conditions. Example
// stack traces:
//   - java.lang.RuntimeException: Expected event (deadLetters,bcast0,NodeReachable(bcast3))
//       at  akka.dispatch.verification.ReplayScheduler.schedule_new_message(ReplayScheduler.scala:215)
//   - assume(pendingFDMessages.isEmpty) does not hold.
// TODO(cs): would be nice force GreedyED to favor deletions rather than
// unexpecteds, since DFS terminates whereas BFS does not necessarily.
// TODO(cs): at some point, GreedyED slows down dramatically. Figure out why.

/**
 * Scheduler that takes greedily tries to minimize edit distance from the
 * original execution.
 *
 * populateActors: whether to populateActors within test(). If false, you
 * the caller needs to do it before invoking test().
 */
class GreedyED(var original_trace: EventTrace, var execution_bound: Int,
               messageFingerprinter: MessageFingerprinter,
               populateActors: Boolean,
               enableFailureDetector: Boolean) extends AbstractScheduler
    with ExternalEventInjector[Event] with TestOracle with HistoricalScheduler {
  assume(!original_trace.isEmpty)
  assume(original_trace.original_externals != null)

  def this(original_trace: EventTrace) =
      this(original_trace, -1, new BasicFingerprinter, true, true)
  def this(original_trace: EventTrace, execution_bound: Int) =
      this(original_trace, execution_bound, new BasicFingerprinter, true, true)

  if (!enableFailureDetector) {
    disableFailureDetector()
  }

  enableCheckpointing()

  var test_invariant : Invariant = null

  // Have we started off the execution yet?
  private[this] var firstMessage = true

  // Current set of enabled events. Includes external messages, but not
  // failure detector messages, which are always sent in FIFO order.
  // (snd, rcv, msg) => Queue(rcv's cell, envelope of message)
  var pendingEvents = new HashMap[(String, String, MessageFingerprint),
                                  Queue[Uniq[(ActorCell, Envelope)]]]

  // Track which timers we have sent (via TimerSend) so that we know whether
  // we should act on the corresponding TimerDelivery.
  // TODO(cs): this complexity may not be necessary, e.g. it's possible that
  // we're guarenteed that TimerSent's are always valid such that
  // TimerDelivery's are also valid. I just haven't thought about it deeply
  // enough, so I'm being conservative.
  val timersSentButNotYetDelivered = new MultiSet[TimerFingerprint]

  // Current set of failure detector messages destined for actors, to be delivered
  // in the order they arrive. Always prioritized over internal messages.
  var pendingFDMessages = new Queue[Uniq[(ActorCell, Envelope)]]

  // The external event subsequence we're currently testing.
  var subseq : Seq[ExternalEvent] = null

  // How many times we have needed to make a choice about whether to drop. This is to
  // allow an optimization: if we pop from the priority queue and discovery that we
  // just added that item to the priority queue, then we don't need to restore the snapshot.
  var currentFork = 0

  // A priority queue for tracking what event we're going to explore next.
  // Tuple type is:
  //   (edit distance, checkpoint, remaining events, message to inject next, fork index)
  // We subtract to get a min-heap rather than a max-heap.
  // If there is a tie, the priority queue acts as a FIFO queue.
  def ordering(t: (Int, EventTrace, Seq[Event], Int)) = -t._1
  var priorityQueue = new PriorityQueue[
    (Int, EventTrace, Seq[Event], Int)]()(Ordering.by(ordering))

  // Our current edit distance from the original trace.
  var ed = 0

  // Whether we are currently backtracking.
  var pausing = new AtomicBoolean(false)

  // Whether we have found the invariant violation, and are in the process of
  // halting the system.
  var foundViolation = new AtomicBoolean(false)

  // The violation fingerprint we're looking for
  var violationFingerprint : ViolationFingerprint = null

  // We can't shutdown() within an actor's thread -- we need a thread outside
  // the actor system to do it for us. This is our signal from the actor
  // system to that thread that a shutdown is required. We initialize the
  // semaphore to 0 rather than 1, so that the shutdown thread blocks upon
  // invoking acquire() until some actor thread release()'s it.
  var restoreCheckpointSemaphore = new Semaphore(0)
  new Thread(new Runnable {
    def run() {
      while (true) {
        // N.B. we assume that when the actor thread release()'s the
        // semaphore, it immediately goes to sleep, i.e. the ActorSystem has
        // stopped dispatching.
        restoreCheckpointSemaphore.acquire
        popPriorityQueue()
      }
    }
  }).start

  // Pre: there is a SpawnEvent for every sender and recipient of every SendEvent
  // Pre: subseq is not empty.
  def test (_subseq: Seq[ExternalEvent],
            _violationFingerprint: ViolationFingerprint) : Boolean = {
    if (!(Instrumenter().scheduler eq this)) {
      throw new IllegalStateException("Instrumenter().scheduler not set!")
    }
    assume(!_subseq.isEmpty)
    subseq = _subseq
    violationFingerprint = _violationFingerprint
    event_orchestrator.events.setOriginalExternalEvents(
        original_trace.original_externals)
    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }

    if (enableFailureDetector) {
      fd.startFD(instrumenter.actorSystem)
    }

    if (populateActors) {
      populateActorSystem(original_trace.getEvents flatMap {
        case SpawnEvent(_,props,name,_) => Some((props, name))
        case _ => None
      })
    }

    // We use the original trace as our reference point as we step through the
    // execution.
    val filtered = original_trace.filterFailureDetectorMessages.
                                  subsequenceIntersection(subseq)
    val updatedEvents = updateEvents(filtered.getEvents)
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
    shutdown()
    // Somewhat confusing: test passes if we failed to find a violation.
    return !foundViolation.get
  }

  private[this] def getUnexpected() : Seq[MsgEvent] = {
    val expected = new MultiSet[MsgEvent]
    // TODO(cs): getNextInterval might be too conservative, i.e. we might want
    // to include all expected events, not just the ones in this interval.
    expected ++= STSScheduler.getNextInterval(
      event_orchestrator.getRemainingTrace()).flatMap {
        // Make sure to fingerprint the expected message
        case m: MsgEvent =>
          Some(MsgEvent(m.sender, m.receiver,
            messageFingerprinter.fingerprint(m.msg)))
        case _ => None
    }

    return IntervalPeekScheduler.unexpected(
        IntervalPeekScheduler.flattenedEnabled(pendingEvents), expected,
        messageFingerprinter)
  }

  // Should only ever be invoked by notify_quiescence, after we have paused
  // the message dispatch loop.
  def popPriorityQueue() : Unit = {
    println("popPriorityQueue")
    val (_ed, prefix, remaining_trace, fork) = priorityQueue.dequeue
    ed = _ed
    // TODO(cs): use fork to break ties, prefering longer event chains to
    // shorter ones.
    currentFork = fork + 1

    if (prefix != event_orchestrator.events) {
      println("restoring checkpoint")
      // Restore the checkpoint.
      // First kill the current actor system.
      shutdown()
      val replayer = new ReplayScheduler(messageFingerprinter, enableFailureDetector, false)
      replayer.eventMapper = eventMapper
      Instrumenter().scheduler = replayer
      replayer.replay(prefix)
      // Now swap out the scheduler mid-execution
      Instrumenter().scheduler = this
      // Grab the replayer's relevant state
      val originalExternals = event_orchestrator.events.original_externals
      event_orchestrator = replayer.event_orchestrator
      event_orchestrator.events.setOriginalExternalEvents(originalExternals)
      pendingEvents = replayer.pendingEvents
      pendingTimers = replayer.pendingTimers
      enqueuedExternalMessages = replayer.enqueuedExternalMessages
      messagesToSend = replayer.messagesToSend
      actorNames = replayer.actorNames
      currentTime = replayer.currentTime
      // Give fd an actual (not no-op) enqueue_message
      if (enableFailureDetector) {
        var fd = replayer.fd
        fd.enqueue_message = enqueue_message
      }
    } else {
      // Else we don't actually need to restore the checkpoint, since we're already
      // here!
      println("Skipped checkpoint restoration!")
    }

    // Reset the remaining expected events.
    event_orchestrator.set_trace(remaining_trace)

    // Get us moving again.
    pausing.set(false)
    firstMessage = true
    println("Unpausing")
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
              if (Instrumenter().timerToCancellable contains ((receiver, message))) {
                Instrumenter().manuallyHandleTick(receiver, message)
              } else {
                enqueue_message(receiver, message)
              }
            }
          case TimerSend(fingerprint) =>
            if (pendingTimers contains fingerprint) {
              val timer = pendingTimers(fingerprint)
              Instrumenter().manuallyHandleTick(fingerprint.receiver, timer)
              timersSentButNotYetDelivered += fingerprint
            }
          case TimerDelivery(fingerprint) =>
            if (timersSentButNotYetDelivered contains fingerprint) {
              timersSentButNotYetDelivered -= fingerprint
              break
            }
          // MsgEvent is the delivery
          case m: MsgEvent =>
            // Check if the event is expected to occur
            def messagePending(m: MsgEvent) : Boolean = {
              // Make sure to send any external messages that recently got enqueued
              send_external_messages(false)
              val key = (m.sender, m.receiver,
                         messageFingerprinter.fingerprint(m.msg))
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

            // Here we have have two choices:
            //  - ignore m
            //  - try some number of unexpected events
            val unexpected = getUnexpected()
            // TODO(cs): treat timers as unexpected messages
            if (!unexpected.isEmpty) {
              println("pushing priorityQueue and pausing..")
              val remaining_trace = event_orchestrator.getRemainingTrace().toList
              // First try ignoring m
              priorityQueue += ((ed+1, event_orchestrator.events.copy, remaining_trace.tail, currentFork))
              // Now try the unexpecteds.
              for (msgEvent <- unexpected) {
                val prepended = msgEvent :: remaining_trace
                priorityQueue += ((ed+1, event_orchestrator.events.copy, prepended, currentFork))
              }
              pausing.set(true)
              break
            }
            // Else, unexpected.isEmpty, and we don't have any choice other
            // than to ignore this message.
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
    val fingerprint = messageFingerprinter.fingerprint(msg)
    val uniq = Uniq[(ActorCell, Envelope)]((cell, envelope))
    event_orchestrator.events.appendMsgSend(snd, rcv, envelope.message, uniq.id)

    handle_event_produced(snd, rcv, envelope) match {
      case ExternalMessage => {
        // We assume that the failure detector and the outside world always
        // have connectivity with all actors, i.e. no failure detector partitions.
        if (MessageTypes.fromFailureDetector(msg)) {
          pendingFDMessages += uniq
        } else {
          val msgs = pendingEvents.getOrElse((snd, rcv, fingerprint),
                              new Queue[Uniq[(ActorCell, Envelope)]])
          pendingEvents((snd, rcv, fingerprint)) = msgs += uniq
        }
      }
      case InternalMessage => {
        // Drop any messages that crosses a partition.
        if (!event_orchestrator.crosses_partition(snd, rcv)) {
          val msgs = pendingEvents.getOrElse((snd, rcv, fingerprint),
                              new Queue[Uniq[(ActorCell, Envelope)]])
          pendingEvents((snd, rcv, fingerprint)) = msgs += uniq
        }
      }
      case _ => None
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
    if (enableFailureDetector) {
      send_external_messages()
      if (!pendingFDMessages.isEmpty) {
        val uniq = pendingFDMessages.dequeue()
        event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
        return Some(uniq.element)
      }
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
        (snd, rcv, messageFingerprinter.fingerprint(msg))
      case TimerDelivery(timer_fingerprint) =>
        val timer = pendingTimers(timer_fingerprint)
        (timer_fingerprint.sender, timer_fingerprint.receiver,
         messageFingerprinter.fingerprint(timer))
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
      assume(pendingFDMessages.isEmpty)
      assume(messagesToSend.isEmpty)
      assume(enqueuedExternalMessages.isEmpty)
      restoreCheckpointSemaphore.release
      return
    }

    assert(started.get)
    started.set(false)
    if (!event_orchestrator.trace_finished) {
      throw new Exception("Shouldn't have stopped!")
    } else {
      if (currentlyInjecting.get) {
        // Check if we found the violation.
        // TODO(cs): is it OK to invoke takeCheckpoint from within an actor
        // thread?
        val checkpoint = takeCheckpoint()
        val violation = test_invariant(subseq, checkpoint)
        var violationFound = false
        violation match {
          case Some(fingerprint) =>
            violationFound = violationFingerprint.matches(fingerprint)
          case _  =>  None
        }
        execution_bound -= 1
        if (violationFound) {
          // Tell the calling thread we are done
          println("Violation found!")
          foundViolation.set(true)
          traceSem.release
        } else if (priorityQueue.isEmpty) {
          println("No more executions to try")
          // Alas, no violation found.
          traceSem.release
        } else if (execution_bound == 0) {
          println("Execution bound exceeded")
          // Alas, no violation found.
          traceSem.release
        } else {
          // Try, try, try again.
          println("No violation found. Trying other paths.")
          assume(pendingFDMessages.isEmpty)
          assume(messagesToSend.isEmpty)
          assume(enqueuedExternalMessages.isEmpty)
          restoreCheckpointSemaphore.release
        }
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
    // handle_before_receive(cell)
  }

  // Called after receive is done being processed
  override def after_receive(cell: ActorCell) {
    // handle_after_receive(cell)
  }

  override def notify_timer_scheduled(sender: ActorRef, receiver: ActorRef,
                                      msg: Any): Boolean = {
    handle_timer_scheduled(sender, receiver, msg, messageFingerprinter)
    // So long as we are following a fixed prefix, we just replay the recorded
    // timer events, and ignore new timer events. IntervalPeekScheduler
    // explores unexpected timer events on our behalf.
    return false
  }

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }
}
