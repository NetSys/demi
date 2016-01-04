package akka.dispatch.verification

import scala.collection.mutable.SynchronizedQueue
import scala.util.Sorting
import scala.reflect.ClassTag


import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

object Aggressiveness extends Enumeration {
  type Level = Value
  // Try all timers for all clusters.
  val NONE = Value
  // Try all timers for the first cluster iteration, then stop
  // as soon as a violation is found for all subsequent clusters.
  val ALL_TIMERS_FIRST_ITR = Value
  // Always stop as soon as a violation is found.
  val STOP_IMMEDIATELY = Value
}

// Cluster events as follows:
// - Cluster all message deliveries according to their Term number.
// - Finding which TimerDeliveries to include is hard [need to reason about
//   actor's current clock values], so instead have a nested for loop:
//   for each cluster we're going to try:
//     for each TimerDelivery:
//        try removing TimerDelivery from list of all TimerDeliveries
//
// See design doc for more info:
//   https://docs.google.com/document/d/1_EqOkSehZVC7oE2hjV1FxY8jw4mXAlM5ESigQYt4ojg/edit
//
// Args:
// - aggressive: whether to stop trying to remove timers as soon as we've
//   found at least one failing violation for the current cluster.
// - skipClockClusters: whether to only explore timers, and just play all
//   clock clusters.
class ClockClusterizer(
    originalTrace: EventTrace,
    fingerprinter: FingerprintFactory,
    resolutionStrategy: AmbiguityResolutionStrategy,
    aggressiveness: Aggressiveness.Level=Aggressiveness.ALL_TIMERS_FIRST_ITR,
    skipClockClusters: Boolean=false) extends Clusterizer {

  val log = LoggerFactory.getLogger("ClockClusterizer")

  // Iteration:
  // - Pick a cluster to remove [starting with removing an empty set]
  // - Wildcard all subsequent clusters
  // - Always include all other events that do not have a Term number

  // Which UniqueMsgEvent ids (other than TimerDeliveries) to include next
  val clusterIterator = new ClockClusterIterator(originalTrace, fingerprinter)
  assert(clusterIterator.hasNext)
  // current set of messages we're *including*
  var currentCluster = clusterIterator.next // Start by not removing any clusters
  var tryingFirstCluster = true

  // Which ids of ElectionTimers we've already tried adding for the current
  // cluster. clear()'ed whenever we update the next cluster to remove.
  // TODO(cs): could optimize this by adding elements that we
  // know must be included.
  var timerIterator = OneAtATimeIterator.fromTrace(originalTrace, fingerprinter)
  // current set of timer ids we're *including*
  var currentTimers = Set[Int]()

  def approximateIterations: Int = {
    if (skipClockClusters) return timerIterator.toRemove.size
    return clusterIterator.clocks.size * timerIterator.toRemove.size
  }

  def getNextTrace(violationReproducedLastRun: Boolean, ignoredAbsentIds: Set[Int])
        : Option[EventTrace] = {
    if (violationReproducedLastRun) {
      timerIterator.producedViolation(currentTimers, ignoredAbsentIds)
      clusterIterator.producedViolation(currentCluster, ignoredAbsentIds)
    }

    if (!timerIterator.hasNext ||
        (aggressiveness == Aggressiveness.ALL_TIMERS_FIRST_ITR &&
            violationReproducedLastRun && !tryingFirstCluster) ||
        (aggressiveness == Aggressiveness.STOP_IMMEDIATELY &&
            violationReproducedLastRun)) {
      tryingFirstCluster = false
      if (!clusterIterator.hasNext || skipClockClusters) {
        return None
      }

      timerIterator.reset
      currentCluster = clusterIterator.next
      log.info("Trying to remove clock cluster: " +
        clusterIterator.inverse(currentCluster).toSeq.sorted)
    }

    assert(timerIterator.hasNext)
    currentTimers = timerIterator.next
    log.info("Trying to remove timers: " + timerIterator.inverse(currentTimers).toSeq.sorted)

    val events = new SynchronizedQueue[Event]
    events ++= originalTrace.events.flatMap {
      case u @ UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) if EventTypes.isExternal(u) =>
        Some(u)
      case UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) =>
        if (currentTimers contains id) {
          Some(UniqueMsgEvent(MsgEvent(snd,rcv,
            WildCardMatch((lst, backtrackSetter) => {
                // Timers get to bypass the resolutionStrategy: allow any match
                val idx = lst.indexWhere(fingerprinter.causesClockIncrement)
                if (idx == -1) None else Some(idx)
              },
              name="CausesClockIncrement")),
            id))
        } else if (currentCluster contains id) {
          val classTag = ClassTag(msg.getClass)
          def messageFilter(pendingMsg: Any): Boolean = {
            ClassTag(pendingMsg.getClass) == classTag
          }
          // Choose the least recently sent message for now.
          Some(UniqueMsgEvent(MsgEvent(snd,rcv,
            WildCardMatch((lst, backtrackSetter) =>
             resolutionStrategy.resolve(messageFilter, lst, backtrackSetter),
             name=classTag.toString)), id))
        } else {
          None
        }
      case _: MsgEvent =>
        throw new IllegalArgumentException("Must be UniqueMsgEvent")
      case t @ UniqueTimerDelivery(_, _) =>
        throw new IllegalArgumentException("TimerDelivery not supported. Replay first")
      case e => Some(e)
    }
    return Some(new EventTrace(events, originalTrace.original_externals))
  }
}

// First iteration always includes all events
class ClockClusterIterator(originalTrace: EventTrace, fingerprinter: FingerprintFactory) {
  val allIds = originalTrace.events.flatMap {
    case m @ UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) =>
      if (!fingerprinter.causesClockIncrement(msg) &&
          !fingerprinter.getLogicalClock(msg).isEmpty) {
        Some(id)
      } else {
        None
      }
    case t @ UniqueTimerDelivery(_, _) =>
      throw new IllegalArgumentException("TimerDelivery not supported. Replay first")
    case e => None
  }.toSet

  // Start by not removing any of the clock clusters, only trying to minimize
  // Timers.
  var firstClusterRemoval = true
  // Then: start with the first non-empty cluster
  var nextClockToRemove : Long = -1
  // Which clock values are safe to remove, i.e. we know that it's possible to
  // trigger the violation if they are removed.
  var blacklist = Set[Int]()

  var clocks : Seq[Long] = Seq.empty
  def computeRemainingClocks(): Seq[Long] = {
    val lowest = clocks.headOption.getOrElse(0:Long)
    return originalTrace.events.flatMap {
      case UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) =>
        if (!(blacklist contains id)) {
          fingerprinter.getLogicalClock(msg)
        } else {
          None
        }
      case _ => None
    }.toSet.toSeq.sorted.dropWhile(c => c < lowest)
  }
  clocks = computeRemainingClocks

  private def current : Set[Int] = {
    val currentClockToRemove : Long = if (firstClusterRemoval) -1 else nextClockToRemove

    originalTrace.events.flatMap {
      case m @ UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) =>
        if (fingerprinter.causesClockIncrement(msg)) {
          // Handled by OneAtATimeIterator
          None
        } else {
          val clock = fingerprinter.getLogicalClock(msg)
          if (clock.isEmpty) {
            Some(id)
          } else {
            val clockVal = clock.get
            if (clockVal == currentClockToRemove || (blacklist contains id)) {
              None
            } else {
              Some(id)
            }
          }
        }
      case t @ UniqueTimerDelivery(_, _) =>
        throw new IllegalArgumentException("TimerDelivery not supported. Replay first")
      case e => None
    }.toSet
  }

  // pre: hasNext
  def next : Set[Int] = {
    if (firstClusterRemoval) {
      val ret = current
      firstClusterRemoval = false
      ret
    } else {
      nextClockToRemove = clocks.head
      val ret = current
      clocks = clocks.tail
      ret
    }
  }

  def hasNext = firstClusterRemoval || !clocks.isEmpty

  def producedViolation(previouslyIncluded: Set[Int], ignoredAbsents: Set[Int]) {
    blacklist = blacklist ++ inverse(previouslyIncluded)
    if (!ignoredAbsents.isEmpty) {
      blacklist = blacklist ++ allIds.intersect(ignoredAbsents)
      clocks = computeRemainingClocks
    }
  }

  def inverse(toInclude: Set[Int]) = allIds -- toInclude
}

object OneAtATimeIterator {
  def fromTrace(originalTrace: EventTrace, fingerprinter: FingerprintFactory): OneAtATimeIterator = {
    var allTimerIds = Set[Int]() ++ originalTrace.events.flatMap {
      case UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) =>
        if (fingerprinter.causesClockIncrement(msg)) {
          Some(id)
        } else {
          None
        }
      case _ => None
    }
    return new OneAtATimeIterator(allTimerIds)
  }
}

// First iteration always includes all timers
// Second iteration includes all timers except the first,
// etc.
class OneAtATimeIterator(all: Set[Int]) {
  var toRemove = all.toSeq.sorted
  var first = true // if first, don't remove any elements
  // Timers that allow the violation to be triggered after having been removed
  var blacklist = Set[Int]()

  private def current = {
    if (first) {
      all -- blacklist
    } else {
      (all - toRemove.head) -- blacklist
    }
  }

  // pre: hasNext
  def next = {
    if (first) {
      val ret = current
      first = false
      ret
    } else {
      val ret = current
      toRemove = toRemove.tail
      ret
    }
  }

  def hasNext = {
    first || !toRemove.isEmpty
  }

  def producedViolation(previouslyIncluded: Set[Int], ignoredAbsents: Set[Int]) {
    blacklist = blacklist ++ inverse(previouslyIncluded) ++
                all.intersect(ignoredAbsents)
  }

  def reset {
    toRemove = (all -- blacklist).toSeq.sorted
    first = true
  }

  def inverse(toInclude: Set[Int]) = all -- toInclude
}
