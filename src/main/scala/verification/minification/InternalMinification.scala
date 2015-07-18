package akka.dispatch.verification

import scala.collection.mutable.SynchronizedQueue
import akka.actor.Props

// Minimizes internal events
// TODO(cs): ultimately, we should try supporting DPOR removal of
// internals.
abstract class InternalEventMinimizer(
  mcs: Seq[ExternalEvent],
  verified_mcs: EventTrace,
  violation: ViolationFingerprint,
  removalStrategy: RemovalStrategy) {

  def minimize(): Tuple2[MinimizationStats, EventTrace]
}

class STSSchedMinimizer(
  mcs: Seq[ExternalEvent],
  verified_mcs: EventTrace,
  violation: ViolationFingerprint,
  removalStrategy: RemovalStrategy,
  schedulerConfig: SchedulerConfig,
  actorNameProps: Seq[Tuple2[Props, String]])
  extends InternalEventMinimizer(mcs, verified_mcs, violation, removalStrategy) {

  def minimize(): Tuple2[MinimizationStats, EventTrace] = {
    val stats = new MinimizationStats("InternalMin", "STSSched")

    val origTrace = verified_mcs.filterCheckpointMessages.filterFailureDetectorMessages
    var lastFailingTrace = origTrace
    // TODO(cs): make this more efficient? Currently O(n^2) overall.
    var nextTrace = removalStrategy.getNextTrace(lastFailingTrace)

    while (!nextTrace.isEmpty) {
      RunnerUtils.testWithStsSched(schedulerConfig, mcs, nextTrace.get, actorNameProps,
                       violation, stats) match {
        case Some(trace) =>
          // Some other events may have been pruned by virtue of being absent. So
          // we reassign lastFailingTrace, then pick then next trace based on
          // it.
          val filteredTrace = trace.filterCheckpointMessages.filterFailureDetectorMessages
          val origSize = RunnerUtils.countMsgEvents(lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages)
          val newSize = RunnerUtils.countMsgEvents(filteredTrace)
          val diff = origSize - newSize
          println("Ignoring worked! Pruned " + diff + "/" + origSize + " deliveries")
          lastFailingTrace = filteredTrace
          lastFailingTrace.setOriginalExternalEvents(mcs)
        case None =>
          // We didn't trigger the violation.
          println("Ignoring didn't work. Trying next")
          None
      }
      nextTrace = removalStrategy.getNextTrace(lastFailingTrace)
    }
    val origSize = RunnerUtils.countMsgEvents(origTrace)
    val newSize = RunnerUtils.countMsgEvents(lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages)
    val diff = origSize - newSize
    println("Pruned " + diff + "/" + origSize + " deliveries in " +
            stats.total_replays + " replays")
    return (stats, lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages)
  }
}

// An "Iterator" for deciding which subsequence of events we should try next.
// Inheritors must implement the "getNextTrace" method.
abstract class RemovalStrategy(verified_mcs: EventTrace, messageFingerprinter: FingerprintFactory) {
  // MsgEvents we've tried ignoring so far. MultiSet to account for duplicate MsgEvent's
  val triedIgnoring = new MultiSet[(String, String, MessageFingerprint)]

  // Populate triedIgnoring with all events that lie between a
  // UnignorableEvents block. Also, external messages.
  private def init() {
    var inUnignorableBlock = false
    verified_mcs.events.foreach {
      case BeginUnignorableEvents =>
        inUnignorableBlock = true
      case EndUnignorableEvents =>
        inUnignorableBlock = false
      case m @ UniqueMsgEvent(MsgEvent(snd, rcv, msg), id) =>
        if (snd == "deadLetters" || inUnignorableBlock) {
          // N.B., for Spark, messages sent from a non-actor
          // should be labeled "external" rather than "deadLetters"
          triedIgnoring += ((snd, rcv, messageFingerprinter.fingerprint(msg)))
        }
      case t @ UniqueTimerDelivery(TimerDelivery(snd, rcv, msg), id) =>
        if (inUnignorableBlock) {
          triedIgnoring += ((snd, rcv, messageFingerprinter.fingerprint(msg)))
        }
    }
  }

  init()

  def getNextTrace(lastFailingTrace: EventTrace) : Option[EventTrace]
}

// TODO(cs): this is a bit redundant with OneAtATimeRemoval + STSSched.
class LeftToRightOneAtATime(
  verified_mcs: EventTrace, messageFingerprinter:  FingerprintFactory)
  extends RemovalStrategy(verified_mcs, messageFingerprinter) {

  // Filter out the next MsgEvent, and return the resulting EventTrace.
  // If we've tried filtering out all MsgEvents, return None.
  def getNextTrace(trace: EventTrace): Option[EventTrace] = {
    // Track what events we've kept so far in this iteration because we
    // already tried ignoring them previously. MultiSet to account for
    // duplicate MsgEvent's. TODO(cs): this may lead to some ambiguous cases.
    val keysThisIteration = new MultiSet[(String, String, MessageFingerprint)]
    // Whether we've found the event we're going to try ignoring next.
    var foundIgnoredEvent = false

    // Return whether we should keep this event
    def checkDelivery(snd: String, rcv: String, msg: Any): Boolean = {
      val key = (snd, rcv, messageFingerprinter.fingerprint(msg))
      keysThisIteration += key
      if (foundIgnoredEvent) {
        // We already chose our event to ignore. Keep all other events.
        return true
      } else {
        // Check if we should ignore or keep this one.
        if (keysThisIteration.count(key) > triedIgnoring.count(key)) {
          // We found something to ignore
          println("Ignoring next: " + key)
          foundIgnoredEvent = true
          triedIgnoring += key
          return false
        } else {
          // Keep this one; we already tried ignoring it, but it was
          // not prunable.
          return true
        }
      }
    }

    // We accomplish two tasks as we iterate through trace:
    //   - Finding the next event we want to ignore
    //   - Filtering (keeping) everything that we don't want to ignore
    val modified = trace.events.flatMap {
      case m @ UniqueMsgEvent(MsgEvent(snd, rcv, msg), id) =>
        if (checkDelivery(snd, rcv, msg)) {
          Some(m)
        } else {
          None
        }
      case t @ UniqueTimerDelivery(TimerDelivery(snd, rcv, msg), id) =>
        if (checkDelivery(snd, rcv, msg)) {
          Some(t)
        } else {
          None
        }
      case e =>
        Some(e)
    }
    if (foundIgnoredEvent) {
      val queue = new SynchronizedQueue[Event]
      queue ++= modified
      return Some(new EventTrace(queue,
                                 verified_mcs.original_externals))
    }
    // We didn't find anything else to ignore, so we're done
    return None
  }
}
