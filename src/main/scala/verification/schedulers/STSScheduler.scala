package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Cell, ActorRef, ActorSystem, Props}

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

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

// TODO(cs): STSSched ignores external WaitQuiescence events. That's a little
// weird, since the minimization routines are feeding different combinations
// of WaitQuiescence events as part of the external event subsequences, yet we
// ignore them altogether..

// TODO(cs): we invoke advanceReplay one too many times when peek is enabled. I
// believe two threads are waiting on schedSemaphore, and both call into
// advanceReplay. For now it looks like the redundant calls into advanceReplay are a no-op,
// but it's possible that this might trigger a bug.

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
class STSScheduler(val schedulerConfig: SchedulerConfig,
                   var original_trace: EventTrace,
                   allowPeek:Boolean=false) extends AbstractScheduler
    with ExternalEventInjector[Event] with TestOracle {

  def getName: String = if (allowPeek) "STSSched" else "STSSchedNoPeek"

  val logger = LoggerFactory.getLogger("STSScheduler")

  val messageFingerprinter = schedulerConfig.messageFingerprinter
  val shouldShutdownActorSystem = schedulerConfig.shouldShutdownActorSystem
  val filterKnownAbsents = schedulerConfig.filterKnownAbsents

  var test_invariant : Invariant = schedulerConfig.invariant_check match {
    case Some(i) => i
    case None => null
  }

  // Have we not started off the execution yet?
  private[this] var firstMessage = true

  // Current set of enabled events. Includes external messages, but not
  // failure detector messages, which are always sent in FIFO order.
  // (snd, rcv, msg) => Queue(rcv's cell, envelope of message)
  val pendingEvents = new HashMap[(String, String, MessageFingerprint),
                                  Queue[Uniq[(Cell, Envelope)]]]

  // Current set of failure detector or CheckpointRequest messages destined for
  // actors, to be delivered in the order they arrive.
  // Always prioritized over internal messages.
  var pendingSystemMessages = new Queue[Uniq[(Cell, Envelope)]]

  // Are we currently pausing the ActorSystem in order to invoke Peek()
  var pausing = new AtomicBoolean(false)

  // Are we currently pausing the ActorSystem in order to hard kill an actor?
  var stopDispatch = new AtomicBoolean(false)

  // Whether the recorded event trace indicates that we should (soon) be in a
  // "UnignoreableEvents" block in the current execution, as indicated by the value
  // of the ExternalEventInjector.unignorableEvents variable
  var expectUnignorableEvents = false

  // Whether the recorded event trace indicates that an external thread should be in a
  // atomic block in the current execution, as indicated by the value
  // of the ExternalEventInjector.externalAtomicBlocks variable.
  // This is really just a sanity check.
  var expectedExternalAtomicBlocks = new HashSet[Long]

  // An optional callback that will be before we execute the trace.
  type PreTestCallback = () => Unit
  var preTestCallback : PreTestCallback = () => None
  def setPreTestCallback(c: PreTestCallback) { preTestCallback = c }

  // An optional callback that will be invoked after we execute the trace.
  type PostTestCallback = () => Unit
  var postTestCallback : PostTestCallback = () => None
  def setPostTestCallback(c: PostTestCallback) { postTestCallback = c }

  // Pre: there is a SpawnEvent for every sender and recipient of every SendEvent
  // Pre: subseq is not empty.
  def test (subseq: Seq[ExternalEvent],
            violationFingerprint: ViolationFingerprint,
            stats: MinimizationStats,
            initializationRoutine:Option[()=>Any]=None) : Option[EventTrace] = {
    assume(!original_trace.isEmpty)
    if (!(Instrumenter().scheduler eq this)) {
      throw new IllegalStateException("Instrumenter().scheduler not set!")
    }
    assume(!subseq.isEmpty)
    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }

    preTestCallback()

    // We only ever replay once
    if (stats != null) {
      stats.increment_replays()
    }

    if (schedulerConfig.enableFailureDetector) {
      fd.startFD(instrumenter.actorSystem)
    }
    if (schedulerConfig.enableCheckpointing) {
      checkpointer.startCheckpointCollector(Instrumenter().actorSystem)
    }

    if (!alreadyPopulated) {
      if (actorNamePropPairs != null) {
        populateActorSystem(actorNamePropPairs)
      } else {
        populateActorSystem(original_trace.getEvents flatMap {
          case SpawnEvent(_,props,name,_) => Some((props, name))
          case _ => None
        })
      }
    }

    // We use the original trace as our reference point as we step through the
    // execution.
    val filtered = original_trace.filterFailureDetectorMessages.
                                  subsequenceIntersection(subseq, filterKnownAbsents=filterKnownAbsents)
    val updatedEvents = filtered.recomputeExternalMsgSends(subseq)
    event_orchestrator.set_trace(updatedEvents)

    if (logger.isTraceEnabled()) {
      println("events:----")
      updatedEvents.zipWithIndex.foreach { case (e,i) => println(i + " " + e) }
      println("----")
    }

    // Bad method name. "reset recorded events"
    event_orchestrator.reset_events

    currentlyInjecting.set(true)

    // Kick off the system's initialization routine
    var initThread : Thread = null
    initializationRoutine match {
      case Some(f) =>
        println("Running initializationRoutine...")
        initThread = new Thread(
          new Runnable { def run() = { f() } },
          "initializationRoutine")
        initThread.start
      case None =>
    }

    // Start playing back trace
    advanceReplay()
    // Have this thread wait until the trace is down. This allows us to safely notify
    // the caller.
    traceSem.acquire
    currentlyInjecting.set(false)
    val checkpoint = if (schedulerConfig.enableCheckpointing) takeCheckpoint() else
                        new HashMap[String, Option[CheckpointReply]]
    val violation = test_invariant(subseq, checkpoint)
    var violationFound = false
    violation match {
      case Some(fingerprint) =>
        violationFound = fingerprint.matches(violationFingerprint)
      case _ => None
    }
    val ret = violationFound match {
      case true => Some(event_orchestrator.events)
      case false => None
    }
    postTestCallback()
    // Wait until the initialization thread is done. Assumes that it
    // terminates!
    if (initThread != null) {
      initThread.join
    }
    reset_all_state
    return ret
  }

  // Should only ever be invoked by notify_quiescence, after we have paused
  // the message dispatch loop.
  def peek() : Unit = {
    val msgEvent : MsgEvent = event_orchestrator.current_event.asInstanceOf[MsgEvent]
    val fingerprintedMsgEvent = MsgEvent(msgEvent.sender, msgEvent.receiver,
        messageFingerprinter.fingerprint(msgEvent.msg))

    val expected = new MultiSet[MsgEvent]
    expected ++= STSScheduler.getNextInterval(
      event_orchestrator.trace.slice(event_orchestrator.traceIdx,
                                     event_orchestrator.trace.length)).flatMap {
      case m: MsgEvent =>
       // Make sure to fingerprint the expected message
       Some(MsgEvent(m.sender, m.receiver,
         messageFingerprinter.fingerprint(m.msg)))
      case _ => None
    }

    // Optimization: if no unexpected events to schedule, give up early.
    val unexpected = IntervalPeekScheduler.unexpected(
        IntervalPeekScheduler.flattenedEnabled(pendingEvents), expected,
        messageFingerprinter)

    if (unexpected.isEmpty) {
      println("No unexpected messages. Ignoring message" + msgEvent)
      event_orchestrator.trace_advanced
      return
    }

    val peeker = new IntervalPeekScheduler(schedulerConfig,
      expected, fingerprintedMsgEvent, 10)

    // N.B. "checkpoint" here means checkpoint of the network's state, as
    // opposed to a checkpoint of the applications state for checking
    // invariants
    Instrumenter().scheduler = peeker
    // N.B. "checkpoint" here means checkpoint of the network's state, as
    // opposed to a checkpoint of the applications state for checking
    // invariants
    val checkpoint = Instrumenter().checkpoint()
    println("Peek()'ing")
    // Make sure to create all actors, not just those with Start events.
    // Prevents tellEnqueue issues.
    val spawns = original_trace.getEvents flatMap {
       case SpawnEvent(_,props,name,_) => Some((props, name))
       case _ => None
    }
    assert(spawns.toSet.size == spawns.length)
    peeker.populateActorSystem(spawns)
    val prefix = peeker.peek(event_orchestrator.events)
    peeker.shutdown
    println("Restoring checkpoint")
    Instrumenter().scheduler = this
    Instrumenter().restoreCheckpoint(checkpoint)
    prefix match {
      case Some(lst) =>
        // Prepend the prefix onto expected events so that
        // schedule_new_message() correctly schedules the prefix.
        println("Found prefix!")
        event_orchestrator.prepend(lst)
      case None =>
        println("No prefix found. Ignoring message" + msgEvent)
        event_orchestrator.trace_advanced
    }
  }

  def advanceReplay() {
    schedSemaphore.acquire
    started.set(true)
    var loop = true
    breakable {
      while (loop && !event_orchestrator.trace_finished) {
        logger.trace("Replaying " + event_orchestrator.traceIdx + "/" +
          event_orchestrator.trace.length + " " + event_orchestrator.current_event)
        event_orchestrator.current_event match {
          case BeginUnignorableEvents =>
            assert(!expectUnignorableEvents)
            expectUnignorableEvents = true
          case EndUnignorableEvents =>
            assert(expectUnignorableEvents)
            expectUnignorableEvents = false
          case BeginExternalAtomicBlock(id) =>
            assert(!(expectedExternalAtomicBlocks contains id))
            expectedExternalAtomicBlocks += id

            // We block until the atomic block has finished
            beganExternalAtomicBlocks.synchronized {
              if (beganExternalAtomicBlocks contains id) {
                endedExternalAtomicBlocks.synchronized {
                  send_external_messages(false)
                  while (!(endedExternalAtomicBlocks contains id)) {
                    println("Blocking until endExternalAtomicBlock("+id+")")
                    // (Releases lock)
                    endedExternalAtomicBlocks.wait()
                    send_external_messages(false)
                  }
                  endedExternalAtomicBlocks -= id
                  beganExternalAtomicBlocks -= id
                }
              } else {
                println("Ignoring externalAtomicBlock("+id+")")
              }
            }
          case EndExternalAtomicBlock(id) =>
            assert(expectedExternalAtomicBlocks contains id)
            expectedExternalAtomicBlocks -= id
          case SpawnEvent (_, _, name, _) =>
            event_orchestrator.trigger_start(name)
          case KillEvent (name) =>
            event_orchestrator.trigger_kill(name)
          case k @ HardKill (name) =>
            // If we just delivered a message to the actor we're about to kill,
            // the current thread is still in the process of
            // handling the Mailbox for that actor. In that
            // case we need to wait for the Mailbox to be set to "Idle" before
            // we can kill the actor, since otherwise the Mailbox will not be
            // able to process the akka-internal "Terminated" messages, i.e.
            // killing it now will result in a deadlock.
            if (Instrumenter().previousActor == name) {
              Instrumenter().dispatchAfterMailboxIdle(name)
              stopDispatch.set(true)
              break
            }
            event_orchestrator.trigger_hard_kill(k)
          case PartitionEvent((a,b)) =>
            event_orchestrator.trigger_partition(a,b)
          case UnPartitionEvent((a,b)) =>
            event_orchestrator.trigger_unpartition(a,b)
          // MsgSend is the initial send
          case m @ MsgSend (sender, receiver, message) =>
            // sender == "deadLetters" means the message is a Send event.
            // N.B. we should have pruned all messages from the failure
            // detector -> actors from event_orchestrator.trace, to ensure
            // that we don't send redundant messages.
            if (sender == "deadLetters") {
              enqueue_message(None, receiver, message)
            }
          case TimerDelivery(snd, rcv, fingerprint) =>
            send_external_messages(false)
            // Check that it was previously delivered, and that it wasn't
            // destined for a dead actor (i.e. dropped by event_produced)
            if (pendingEvents contains (snd, rcv, fingerprint)) {
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
                  // The message is pending, but also double check that the
                  // destination isn't currently blocked.
                  !queue.isEmpty && !(Instrumenter().blockedActors contains m.receiver)
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
              // If it isn't enabled yet, let's try Peek()'ing for it.
              // First, we need to pause the ActorSystem.
              println("Pausing")
              pausing.set(true)
              break
            }

            // OK, the message we expected is not here.
            //
            // Check if we should not ignore it because of an
            // UnignorableEvents block
            if (expectUnignorableEvents) {
              // N.B. it should always be a dispatcher thread (or the main thread)
              // that calls advanceReplay(), so we should be guarenteed that
              // schedule_new_message will not be invoked while we are
              // blocked here.
              messagesToSend.synchronized {
                while (!messagePending(m)) {
                  println("Blocking until enqueue_message...")
                  messagesToSend.wait()
                  println("Checking messagePending..")
                  // N.B. messagePending(m) invokes send_external_messages
                }
              }
              // Yay, it became enabled.
              break
            }

            println("Ignoring message " + m)
          case Quiescence =>
            // This is just a nop. Do nothing
            event_orchestrator.events += Quiescence
          case BeginWaitQuiescence =>
            event_orchestrator.events += BeginWaitQuiescence
            event_orchestrator.trace_advanced
            break
          case c @ CodeBlock(block) =>
            event_orchestrator.events += c // keep the id the same
            // Since the block might send messages, make sure that we treat the
            // message sends as if they are being triggered by an external
            // thread, i.e. we enqueue them rather than letting them be sent
            // immediately.
            Instrumenter.overrideInternalThreadRule
            block()
            Instrumenter.unsetInternalThreadRuleOverride
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

  def event_produced(cell: Cell, envelope: Envelope) = {
    var snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    if (logger.isTraceEnabled()) {
      logger.trace("event_produced: " + snd + " -> " + rcv + " " + msg)
    }

    val fingerprint = messageFingerprinter.fingerprint(msg)
    val uniq = Uniq[(Cell, Envelope)]((cell, envelope))
    var isTimer = false

    handle_event_produced(snd, rcv, envelope) match {
      case ExternalMessage => {
        // We assume that the failure detector / checkpointer and the outside world always
        // have connectivity with all actors, i.e. no failure detector partitions.
        if (MessageTypes.fromFailureDetector(msg) ||
            MessageTypes.fromCheckpointCollector(msg)) {
          pendingSystemMessages += uniq
        } else {
          val msgs = pendingEvents.getOrElse((snd, rcv, fingerprint),
                              new Queue[Uniq[(Cell, Envelope)]])
          pendingEvents((snd, rcv, fingerprint)) = msgs += uniq
        }
      }
      case InternalMessage => {
        if (snd == "deadLetters") {
          isTimer = true
        }
        // Drop any messages that crosses a partition.
        if (!event_orchestrator.crosses_partition(snd, rcv)) {
          val msgs = pendingEvents.getOrElse((snd, rcv, fingerprint),
                              new Queue[Uniq[(Cell, Envelope)]])
          pendingEvents((snd, rcv, fingerprint)) = msgs += uniq
        }
      }
      case _ => None
    }

    // Record this MsgSend as a special if it was sent from a timer.
    snd = if (isTimer) "Timer" else snd
    event_orchestrator.events.appendMsgSend(snd, rcv, envelope.message, uniq.id)
  }

  // Record a mapping from actor names to actor refs
  override def event_produced(event: Event) = {
    handle_spawn_produced(event)
    super.event_produced(event)
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
  def schedule_new_message(blockedActors: Set[String]) : Option[(Cell, Envelope)] = {
    if (stopDispatch.get()) {
      // Return None to stop dispatching.
      return None
    }

    if (pausing.get) {
      // Return None to stop dispatching.
      return None
    }

    // Flush checkpoint/fd messages before proceeding with other messages.
    send_external_messages()
    if (!pendingSystemMessages.isEmpty) {
      // Find a non-blocked destination
      Util.find_non_blocked_message[Uniq[(Cell, Envelope)]](
        blockedActors,
        pendingSystemMessages,
        () => pendingSystemMessages.dequeue(),
        (e: Uniq[(Cell, Envelope)]) => e.element._1.self.path.name) match {
        case Some(uniq) =>
          event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
          return Some(uniq.element)
        case None =>
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
      case TimerDelivery(snd, rcv, timer_fingerprint) =>
        (snd, rcv, timer_fingerprint)
      case _ =>
        // We've broken out of advanceReplay() because of a
        // BeginWaitQuiescence event, but there were no pendingSystemMessages to
        // send. So, we need to invoke advanceReplay() once again to get us up
        // to a pending MsgEvent.
        schedSemaphore.release
        return schedule_new_message(blockedActors)
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

        if (logger.isTraceEnabled()) {
          val cell = uniq.element._1
          val envelope = uniq.element._2
          val snd = envelope.sender.path.name
          val rcv = cell.self.path.name
          val msg = envelope.message
          logger.trace("schedule_new_message(): " + snd + " -> " + rcv + " " + msg)
        }

        // If execution ended, don't schedule!
        if (Instrumenter()._passThrough.get) {
          println("Execution ended! Not proceeding with schedule_new_message")
          schedSemaphore.release()
          traceSem.release()
          return None
        }

        schedSemaphore.release
        return Some(uniq.element)
      case None =>
        throw new RuntimeException("We expected " + key + " to be enabled..")
    }
  }

  override def notify_quiescence () {
    if (stopDispatch.get()) {
      stopDispatch.set(false)
      return
    }

    if (pausing.get) {
      // We've now paused the ActorSystem. Go ahead with peek()
      peek()
      // Get us moving again.
      pausing.set(false)
      firstMessage = true
      advanceReplay()
      return
    }
    assert(started.get)
    started.set(false)

    if (blockedOnCheckpoint.get) {
      checkpointSem.release()
      return
    }

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
  override def before_receive(cell: Cell) {
    super.before_receive(cell)
    handle_before_receive(cell)
  }

  // Called after receive is done being processed
  override def after_receive(cell: Cell) {
    handle_after_receive(cell)
  }

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }

  def notify_timer_cancel(receiver: ActorRef, msg: Any) : Unit = {
    if (handle_timer_cancel(receiver, msg)) {
      return
    }
    val rcv = receiver.path.name
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

  override def enqueue_timer(receiver: String, msg: Any) { handle_timer(receiver, msg) }

  override def actorTerminated(name: String): Seq[(String, Any)] = {
    val result = new Queue[(String, Any)]
    // TODO(cs): also deal with pendingSystemMessages
    for ((snd,rcv,fingerprint) <- pendingEvents.keys) {
      if (rcv == name) {
        val queue = pendingEvents((snd,rcv,fingerprint))
        for (e <- queue) {
          result += ((snd, e.element._2.message))
        }
        pendingEvents -= ((snd,rcv,fingerprint))
      }
    }
    return result
  }

  override def reset_all_state() {
    super.reset_all_state
    reset_state(shouldShutdownActorSystem)
    firstMessage = true
    pendingEvents.clear
    pendingSystemMessages.clear
    expectUnignorableEvents = false
    expectedExternalAtomicBlocks = new HashSet[Long]
    pausing = new AtomicBoolean(false)
    stopDispatch.set(false)
  }
}
