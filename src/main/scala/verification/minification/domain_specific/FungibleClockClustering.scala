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

// TODO(cs): major optimization: if some of the WildCardMatches are absent, i.e.
// skipped over, yet the violation is triggered, we should remove those.

// TODO(cs): SrcDstFIFOOnly technically isn't enough to ensure FIFO removal --
// currently ClockClusterizer can violate the FIFO scheduling discipline.

// When there are multiple pending messages between a given src,dst pair,
// choose how the WildCardMatch's message selector should be applied to the
// pending messages.
trait AmbiguityResolutionStrategy {
  type MessageSelector = (Any) => Boolean

  // Return the index of the selected message, if any.
  def resolve(msgSelector: MessageSelector, pending: Seq[Any]) : Option[Int]
}

class SrcDstFIFOOnly extends AmbiguityResolutionStrategy {
  // If the first pending message doesn't match, give up.
  // For Timers though, allow any of them to matched.
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

// TODO(cs): implement SrcDstFIFO strategy that adds backtrack point for
// playing unexpected events at the front of the FIFO queue whenever the WildCard
// doesn't match the head (but a pending event later does?).
// Key constraint: need to make sure that we're always making progress, i.e.
// what we're exploring would be shorter if it pans out.

// TODO(cs): For UDP bugs: adds a backtrack point to try a
// different match if there are multiple ambiguous pending matches.

object TestScheduler extends Enumeration {
  type TestScheduler = Value
  val STSSched, DPORwHeuristics = Value
}

// Domain-specific strategy. See design doc:
//   https://docs.google.com/document/d/1_EqOkSehZVC7oE2hjV1FxY8jw4mXAlM5ESigQYt4ojg/edit
class FungibleClockMinimizer(
  schedulerConfig: SchedulerConfig,
  mcs: Seq[ExternalEvent],
  trace: EventTrace,
  actorNameProps: Seq[Tuple2[Props, String]],
  violation: ViolationFingerprint,
  testScheduler:TestScheduler.TestScheduler=TestScheduler.STSSched,
  depGraph: Option[Graph[Unique,DiEdge]]=None,
  resolutionStrategy:AmbiguityResolutionStrategy=new SrcDstFIFOOnly,
  initializationRoutine: Option[() => Any]=None,
  preTest: Option[STSScheduler.PreTestCallback]=None,
  postTest: Option[STSScheduler.PostTestCallback]=None)
  extends InternalEventMinimizer {

  def testWithDpor(nextTrace: EventTrace, stats: MinimizationStats): Option[EventTrace] = {
    val uniques = new Queue[Unique] ++ nextTrace.events.flatMap {
      case UniqueMsgEvent(m @ MsgEvent(snd,rcv,wildcard), id) =>
        Some(Unique(m,id=(-1)))
      case s: SpawnEvent => None // DPOR ignores SpawnEvents
      case m: UniqueMsgSend => None // DPOR ignores MsgEvents
      case BeginWaitQuiescence => None // DPOR ignores BeginWaitQuiescence
      case Quiescence => None // forget about it for now?
      case c: ChangeContext => None
      // TODO(cs): deal with Partitions, etc.
    }

    // TODO(cs): keep the same dpor instance across runs.
    val dpor = new DPORwHeuristics(schedulerConfig,
                        prioritizePendingUponDivergence=true,
                        backtrackHeuristic=new StopImmediatelyOrdering,
                        stopIfViolationFound=false,
                        startFromBackTrackPoints=false,
                        skipBacktrackComputation=true,
                        stopAfterNextTrace=true)
    depGraph match {
      case Some(g) => dpor.setInitialDepGraph(g)
      case None =>
    }
    dpor.setMaxDistance(0)
    dpor.setMaxMessagesToSchedule(uniques.size)
    dpor.setActorNameProps(actorNameProps)
    val filtered_externals = DPORwHeuristicsUtil.convertToDPORTrace(mcs,
      actorNameProps.map(t => t._2), false)

    dpor.setInitialTrace(uniques)
    return dpor.test(filtered_externals, violation, stats)
  }

  def testWithSTSSched(nextTrace: EventTrace, stats: MinimizationStats): Option[EventTrace] = {
    stats.increment_replays

    return RunnerUtils.testWithStsSched(
      schedulerConfig,
      mcs,
      nextTrace,
      actorNameProps,
      violation,
      stats,
      initializationRoutine=initializationRoutine,
      preTest=preTest,
      postTest=postTest)
  }

  // N.B. for best results, run RunnerUtils.minimizeInternals on the result if
  // we managed to remove anything here.
  def minimize(): Tuple2[MinimizationStats, EventTrace] = {
    val stats = new MinimizationStats("FungibleClockMinimizer", "STSSched")
    val clockClusterizer = new ClockClusterizer(trace,
      schedulerConfig.messageFingerprinter, resolutionStrategy)

    var minTrace = trace

    var nextTrace = clockClusterizer.getNextTrace(false)
    while (!nextTrace.isEmpty) {
      val ret = if (testScheduler == TestScheduler.DPORwHeuristics)
        testWithDpor(nextTrace.get, stats)
        else testWithSTSSched(nextTrace.get, stats)

      if (!ret.isEmpty) {
        println("Pruning was successful.")
        if (ret.get.size < minTrace.size) {
          minTrace = ret.get
        }
      }
      nextTrace = clockClusterizer.getNextTrace(!ret.isEmpty)
    }

    if (testScheduler == TestScheduler.DPORwHeuristics) {
      // DPORwHeuristics doesn't play nicely with other schedulers, so we need
      // to clean up after it.
      // Counterintuitively, use a dummy Replayer to do this, since
      // DPORwHeuristics doesn't have shutdownSemaphore.
      val replayer = new ReplayScheduler(schedulerConfig, false)
      Instrumenter().scheduler = replayer
      replayer.shutdown()
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
            WildCardMatch((lst) => {
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
