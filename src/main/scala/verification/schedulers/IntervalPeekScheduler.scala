package akka.dispatch.verification

import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Semaphore

// TODO(cs): try placing newly scheduled timer events at the front of
// pendingUnepxectedEvents, rather than at the back like other messages.

object IntervalPeekScheduler {
  // N.B. enabled should contain non-fingerprinted messages, whereas _expected
  // should contain fingerprinted messages
  def unexpected(enabled: Seq[MsgEvent], _expected: MultiSet[MsgEvent],
                 pendingTimers: Seq[(String, Any)], messageFingerprinter: MessageFingerprinter) : Seq[MsgEvent] = {
    // TODO(cs): consider ordering of expected, rather than treating it as a Set?
    val expected = new MultiSet[MsgEvent] ++ _expected
    def fingerprintAndMatch(e: MsgEvent): Boolean = {
      val fingerprinted = MsgEvent(e.sender, e.receiver,
        messageFingerprinter.fingerprint(e.msg))
      expected.contains(fingerprinted) match {
       case true =>
         expected.remove(fingerprinted)
         return false
       case false =>
         // Unexpected
         return true
      }
    }
    val timerEvents = pendingTimers.map(pair => MsgEvent("deadLetters", pair._1, pair._2))
    return enabled.filter(fingerprintAndMatch) ++ timerEvents.filter(fingerprintAndMatch)
  }

  // Flatten all enabled events into a sorted list of (raw, non-fingerprinted) MsgEvents
  def flattenedEnabled(pendingEvents: HashMap[
      (String, String, MessageFingerprint), Queue[Uniq[(ActorCell, Envelope)]]]) : Seq[MsgEvent] = {
    // Last element of queue's tuple is the unique id, which is assumed to be
    // monotonically increasing by time of arrival.
    val unsorted = new Queue[(String, String, Any, Int)]
    pendingEvents.foreach {
      case (key, queue) =>
        for (uniq <- queue) {
          if (key._2 != FailureDetector.fdName) {
            unsorted += ((key._1, key._2, uniq.element._2.message, uniq.id))
          }
        }
    }
    return unsorted.sortBy[Int](tuple => tuple._4).
                    map(tuple => MsgEvent(tuple._1, tuple._2, tuple._3))
  }
}

/**
 * Similar to PeekScheduler(), except that:
 *  a. we start from mid-way in the execution (or rather, we restore a checkpoint of
 *     the state mid-way through the execution, by replaying all events that led up
 *     to that point),
 *  b. we only Peek() for a small interval (up to the next external event),
 *  c. and we schedule in FIFO order rather than RR.
 *
 * Formal contract:
 *   Schedules a fixed number of unexpected messages in FIFO order
 *   to guess whether a particular message i is going to
 *   become enabled or not. If i does become enabled,
 *   we return the unexpected messages that lead up
 *   to its being enabled. Otherwise, return null.
 */
// N.B. both expected and lookingFor should have their msg fields as a MessageFingerprint instead
// of the raw message.
class IntervalPeekScheduler(expected: MultiSet[MsgEvent], lookingFor: MsgEvent,
                            max_peek_messages: Int,
                            messageFingerprinter: MessageFingerprinter,
                            enableFailureDetector: Boolean) extends
      ReplayScheduler(messageFingerprinter, enableFailureDetector, false) {

  def this(expected: MultiSet[MsgEvent], lookingFor: MsgEvent) =
      this(expected, lookingFor, 10, new BasicFingerprinter, true)

  if (!enableFailureDetector) {
    disableFailureDetector
  }

  // Whether we are currently restoring the checkpoint (by replaying a prefix
  // of events), or have moved on to peeking.
  val doneReplayingPrefix = new AtomicBoolean(false)

  // Semaphore to wait for peek to be done. We initialize the
  // semaphore to 0 rather than 1, so that the main thread blocks upon
  // invoking acquire() until another thread release()'s it.
  var donePeeking = new Semaphore(0)

  // Whether we ended up finding lookingFor
  val foundLookingFor = new AtomicBoolean(false)

  // Unexpected messages we have scheduled so far in search of lookingFor
  val postfix = new Queue[MsgEvent]

  // FIFO queue of unexpected events.
  var pendingUnexpectedEvents = new Queue[(ActorCell, Envelope)]

  /*
   * peek() schedules a fixed number of unexpected messages in FIFO order
   * to guess whether a particular message event m is going to become enabled or not.
   *
   * If msg does become enabled, return a prefix of messages that lead up to its being enabled.
   *
   * Otherwise, return None.
   */
  def peek(prefix: EventTrace) : Option[Seq[MsgEvent]] = {
    if (!(Instrumenter().scheduler eq this)) {
      throw new IllegalStateException("Instrumenter().scheduler not set!")
    }
    doneReplayingPrefix.set(false)
    replay(prefix)
    println("Done replaying prefix. Proceeding with peek")
    started.set(true)
    doneReplayingPrefix.set(true)

    // Feed the unexpected events present at the end of replay into
    // pendingUnexpectedEvents.
    val unexpected = IntervalPeekScheduler.unexpected(
        IntervalPeekScheduler.flattenedEnabled(pendingEvents), expected,
        List.empty, messageFingerprinter)
    for (msgEvent <- unexpected) {
      val key = (msgEvent.sender, msgEvent.receiver,
                 messageFingerprinter.fingerprint(msgEvent.msg))
      val nextMessage = pendingEvents.get(key) match {
        case Some(queue) =>
          val willRet = queue.dequeue()
          if (queue.isEmpty) {
            pendingEvents.remove(key)
          }
          Some(willRet)
        case None =>
          // Message not enabled
          None
      }
      nextMessage match {
        case Some(uniq) => pendingUnexpectedEvents += uniq.element
        case None => throw new RuntimeException("Shouldn't happen")
      }
    }

    Instrumenter().start_dispatch

    // Wait for peek scheduling to finish:
    donePeeking.acquire
    if (foundLookingFor.get) {
      return Some(postfix)
    }
    return None
  }

  override def event_produced(cell: ActorCell, envelope: Envelope) : Unit = {
    if (!doneReplayingPrefix.get) {
      return super.event_produced(cell, envelope)
    }

    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    val event = MsgEvent(snd, rcv, msg)
    val fingerprintedEvent = MsgEvent(snd, rcv,
      messageFingerprinter.fingerprint(msg))
    if (expected.contains(fingerprintedEvent)) {
      expected -= fingerprintedEvent
    } else if (fingerprintedEvent == lookingFor) {
      foundLookingFor.set(true)
    } else if (!event_orchestrator.crosses_partition(snd, rcv)) {
      pendingUnexpectedEvents += ((cell, envelope))
    }
  }

  override def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    if (!doneReplayingPrefix.get) {
      return super.schedule_new_message
    }

    if (foundLookingFor.get) {
      return None
    }

    // Send any pending timers.
    send_external_messages()

    if (pendingUnexpectedEvents.isEmpty) {
      // Before giving up, try to see if there are any pending timers. If so,
      // pick one.
      // TODO(cs): somewhat arbitrary to only trigger timers after
      // pendingUnexpectedEvents.isEmpty. Why not before?
      getRandomPendingTimer() match {
        case Some((receiver, msg)) =>
          Instrumenter().manuallyHandleTick(receiver, msg)
          send_external_messages()
          if (pendingUnexpectedEvents.isEmpty) {
            return None
          }
        case None =>
          println("No more events to schedule..")
          return None
      }
    }

    val next = pendingUnexpectedEvents.dequeue()
    val snd = next._2.sender.path.name
    val rcv = next._1.self.path.name
    val msg = next._2.message
    val event = MsgEvent(snd, rcv, msg)
    postfix += event
    if (postfix.length > max_peek_messages) {
      println("Reached maximum unexpected messages")
      return None
    }

    return Some(next)
  }

  override def notify_quiescence() : Unit = {
    if (!doneReplayingPrefix.get) {
      return super.notify_quiescence
    }
    // Wake up the main thread.
    donePeeking.release
  }

  override def notify_timer_scheduled(sender: ActorRef, receiver: ActorRef,
                                      msg: Any): Boolean = {
    handle_timer_scheduled(sender, receiver, msg, messageFingerprinter)
    return doneReplayingPrefix.get()
  }

  override def shutdown () = {
    // Don't restart the system (as in the other schedulers), just shut it
    // down.
    Instrumenter().shutdown_system(false)
  }
}
