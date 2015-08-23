package akka.dispatch.verification

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.SynchronizedQueue
import akka.actor.Props

import scala.util.Sorting
import scala.reflect.ClassTag

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

// When there are multiple pending messages between a given src,dst pair,
// choose how the WildCardMatch's message selector should be applied to the
// pending messages.
trait AmbiguityResolutionStrategy {
  type MessageSelector = (Any) => Boolean

  // Return the index of the selected message, if any.
  def resolve(msgSelector: MessageSelector, pending: Seq[Any]) : Option[Int]
}

// N.B. should only be used with STSSched. DPORwHeuristics doesn't guarentee
// that pending messages are presented in order of delivery time.
class SrcDstFIFOOnly extends AmbiguityResolutionStrategy {
  // If the first pending message doesn't match, give up.
  def resolve(msgSelector: MessageSelector, pending: Seq[Any]) : Option[Int] = {
    pending.headOption match {
      case Some(msg) =>
        if (msgSelector(msg))
          Some(0)
        else
          None
      case None =>
        None
    }
  }
}

// Domain-specific strategy. See design doc:
//   https://docs.google.com/document/d/1_EqOkSehZVC7oE2hjV1FxY8jw4mXAlM5ESigQYt4ojg/edit
class FungibleClockMinimizer(
  schedulerConfig: SchedulerConfig,
  mcs: Seq[ExternalEvent],
  trace: EventTrace,
  actorNameProps: Seq[Tuple2[Props, String]],
  violation: ViolationFingerprint,
  resolutionStrategy:AmbiguityResolutionStrategy=new SrcDstFIFOOnly,
  initializationRoutine: Option[() => Any]=None,
  preTest: Option[STSScheduler.PreTestCallback]=None,
  postTest: Option[STSScheduler.PostTestCallback]=None)
  extends InternalEventMinimizer {

  // N.B. for best results, run RunnerUtils.minimizeInternals on the result if
  // we managed to remove anything here.
  def minimize(): Tuple2[MinimizationStats, EventTrace] = {
    val stats = new MinimizationStats("FungibleClockMinimizer", "STSSched")
    val clockClusterizer = new ClockClusterizer(trace,
      schedulerConfig.messageFingerprinter, resolutionStrategy)

    var minTrace = trace

    var nextTrace = clockClusterizer.getNextTrace(false)
    while (!nextTrace.isEmpty) {
      stats.increment_replays

      val ret = RunnerUtils.testWithStsSched(
        schedulerConfig,
        mcs,
        nextTrace.get,
        actorNameProps,
        violation,
        stats,
        initializationRoutine=initializationRoutine,
        preTest=preTest,
        postTest=postTest)

      if (!ret.isEmpty && ret.get.size < minTrace.size) {
        minTrace = ret.get
      }
      nextTrace = clockClusterizer.getNextTrace(!ret.isEmpty)
    }

    return (stats, minTrace)
  }
}

class ClockClusterizer(
    originalTrace: EventTrace,
    fingerprinter: FingerprintFactory,
    resolutionStrategy: AmbiguityResolutionStrategy) {

  // Clustering:
  // - Cluster all message deliveries according to their Term number.
  // - Finding which TimerDeliveries to include is hard [need to reason about
  //   actor's current clock values], so instead try just removing one at a time
  //   until a violation is found, for each cluster we're going to try
  //   removing.

  // Iteration:
  // - Pick a cluster to remove [starting with removing an empty set]
  // - Wildcard all subsequent clusters
  // - Always include all other events that do not have a Term number

  // Which UniqueMsgEvent ids (other than TimerDeliveries) to include next
  val clusterIterator = new ClockClusterIterator(originalTrace, fingerprinter)
  assert(clusterIterator.hasNext)
  var currentCluster = clusterIterator.next // Start by not removing any clusters

  // Which ids of ElectionTimers we've already tried adding for the current
  // cluster. clear()'ed whenever we update the next cluster to remove.
  // TODO(cs): could optimize this by adding elements that we
  // know must be included.
  var timerIterator = OneAtATimeIterator.fromTrace(originalTrace, fingerprinter)
  var currentTimers = Set[Int]()

  def getNextTrace(violationReproducedLastRun: Boolean) : Option[EventTrace] = {
    if (violationReproducedLastRun) {
      timerIterator.producedViolation(currentTimers)
      clusterIterator.producedViolation(currentCluster)
    }

    if (!timerIterator.hasNext) {
      if (!clusterIterator.hasNext) {
        return None
      }

      timerIterator.reset
      currentCluster = clusterIterator.next
      println("Trying to remove clock cluster: " +
        clusterIterator.inverse(currentCluster).toSeq.sorted)
    }

    assert(timerIterator.hasNext)
    currentTimers = timerIterator.next
    println("Trying to remove timers: " + timerIterator.inverse(currentTimers).toSeq.sorted)

    val events = new SynchronizedQueue[Event]
    events ++= originalTrace.events.flatMap {
      case UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) =>
        if (currentTimers contains id) {
          Some(UniqueMsgEvent(MsgEvent(snd,rcv,
            WildCardMatch((lst) =>
              resolutionStrategy.resolve(fingerprinter.causesClockIncrement, lst))),
            id))
        } else if (currentCluster contains id) {
          val classTag = ClassTag(msg.getClass)
          def messageFilter(pendingMsg: Any): Boolean = {
            ClassTag(pendingMsg.getClass) == classTag
          }
          // Choose the least recently sent message for now.
          Some(UniqueMsgEvent(MsgEvent(snd,rcv,
            WildCardMatch((lst) =>
             resolutionStrategy.resolve(messageFilter, lst),
             name=classTag.toString)), id))
        } else {
          None
        }
      case t @ UniqueTimerDelivery(_, _) =>
        throw new IllegalArgumentException("TimerDelivery not supported. Replay first")
      case e => Some(e)
    }
    return Some(new EventTrace(events, originalTrace.original_externals))
  }
}

// First iteration always includes all events
class ClockClusterIterator(originalTrace: EventTrace, fingerprinter: FingerprintFactory) {
  var clocks = originalTrace.flatMap {
    case MsgEvent(snd,rcv,msg) =>
      fingerprinter.getLogicalClock(msg)
    case _ => None
  }.toSet.toSeq.sorted

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

  def producedViolation(previouslyIncluded: Set[Int]) {
    blacklist = blacklist ++ inverse(previouslyIncluded)
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

  def producedViolation(previouslyIncluded: Set[Int]) {
    blacklist = blacklist ++ inverse(previouslyIncluded)
  }

  def reset {
    toRemove = (all -- blacklist).toSeq.sorted
    first = true
  }

  def inverse(toInclude: Set[Int]) = all -- toInclude
}
