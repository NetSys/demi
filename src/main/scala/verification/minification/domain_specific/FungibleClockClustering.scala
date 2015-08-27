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

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

// TODO(cs): SrcDstFIFOOnly technically isn't enough to ensure FIFO removal --
// currently ClockClusterizer can violate the FIFO scheduling discipline.

// When there are multiple pending messages between a given src,dst pair,
// choose how the WildCardMatch's message selector should be applied to the
// pending messages.
trait AmbiguityResolutionStrategy {
  type MessageSelector = (Any) => Boolean

  // Return the index of the selected message, if any.
  def resolve(msgSelector: MessageSelector, pending: Seq[Any],
              backtrackSetter: (Int) => Unit) : Option[Int]
}

class SrcDstFIFOOnly extends AmbiguityResolutionStrategy {
  // If the first pending message doesn't match, give up.
  // For Timers though, allow any of them to matched.
  def resolve(msgSelector: MessageSelector, pending: Seq[Any],
              backtrackSetter: (Int) => Unit) : Option[Int] = {
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

// For UDP bugs: set a backtrack point every time there are multiple pending
// messages of the same type, and have DPORwHeuristics go back and explore
// those.
class BackTrackStrategy extends AmbiguityResolutionStrategy {
  def resolve(msgSelector: MessageSelector, pending: Seq[Any],
              backtrackSetter: (Int) => Unit) : Option[Int] = {
    val matching = pending.zipWithIndex.filter(
      { case (msg,i) =>msgSelector(msg) })

    if (!matching.isEmpty) {
      // Set backtrack points, if any, for any messages that are of the same
      // type, but not exactly the same as eachother.
      val alreadyTried = new HashSet[Any]
      alreadyTried += matching.head._1
      matching.tail.filter {
        case (msg,i) =>
          if (!(alreadyTried contains msg)) {
            alreadyTried += msg
            true
          } else {
            false
          }
      }.foreach {
        case (msg,i) => backtrackSetter(i)
      }

      return matching.headOption.map(t => t._2)
    }

    return None
  }
}

object TestScheduler extends Enumeration {
  type TestScheduler = Value
  val STSSched = Value
  val DPORwHeuristics = Value
}

// Domain-specific strategy. See design doc:
//   https://docs.google.com/document/d/1_EqOkSehZVC7oE2hjV1FxY8jw4mXAlM5ESigQYt4ojg/edit
class FungibleClockMinimizer(
  schedulerConfig: SchedulerConfig,
  mcs: Seq[ExternalEvent],
  trace: EventTrace,
  actorNameProps: Seq[Tuple2[Props, String]],
  violation: ViolationFingerprint,
  resolutionStrategy: AmbiguityResolutionStrategy=new BackTrackStrategy,
  testScheduler:TestScheduler.TestScheduler=TestScheduler.STSSched,
  depGraph: Option[Graph[Unique,DiEdge]]=None,
  initializationRoutine: Option[() => Any]=None,
  preTest: Option[STSScheduler.PreTestCallback]=None,
  postTest: Option[STSScheduler.PostTestCallback]=None)
  extends InternalEventMinimizer {

  val log = LoggerFactory.getLogger("WildCardMin")

  def testWithDpor(nextTrace: EventTrace, stats: MinimizationStats,
                   absentIgnored: STSScheduler.IgnoreAbsentCallback,
                   resetCallback: DPORwHeuristics.ResetCallback): Option[EventTrace] = {
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

    // TODO(cs): keep the same dpor instance across runs, so that we don't
    // explore reduant executions.
    val dpor = new DPORwHeuristics(schedulerConfig,
                        prioritizePendingUponDivergence=true,
                        stopIfViolationFound=false,
                        startFromBackTrackPoints=false,
                        skipBacktrackComputation=true,
                        stopAfterNextTrace=true)
    dpor.setIgnoreAbsentCallback(absentIgnored)
    dpor.setResetCallback(resetCallback)
    depGraph match {
      case Some(g) => dpor.setInitialDepGraph(g)
      case None =>
    }
    // dpor.setMaxDistance(0) x backtrack points are only set explicitly by us

    dpor.setMaxMessagesToSchedule(uniques.size)
    dpor.setActorNameProps(actorNameProps)
    val filtered_externals = DPORwHeuristicsUtil.convertToDPORTrace(mcs,
      actorNameProps.map(t => t._2), false)

    dpor.setInitialTrace(uniques)
    return dpor.test(filtered_externals, violation, stats)
  }

  def testWithSTSSched(nextTrace: EventTrace, stats: MinimizationStats,
                       absentIgnored: STSScheduler.IgnoreAbsentCallback): Option[EventTrace] = {
    return RunnerUtils.testWithStsSched(
      schedulerConfig,
      mcs,
      nextTrace,
      actorNameProps,
      violation,
      stats,
      initializationRoutine=initializationRoutine,
      preTest=preTest,
      postTest=postTest,
      absentIgnored=Some(absentIgnored))
  }

  // N.B. for best results, run RunnerUtils.minimizeInternals on the result if
  // we managed to remove anything here.
  def minimize(): Tuple2[MinimizationStats, EventTrace] = {
    val stats = new MinimizationStats("FungibleClockMinimizer", "STSSched")
    val clockClusterizer = new ClockClusterizer(trace,
      schedulerConfig.messageFingerprinter, resolutionStrategy)

    var minTrace = trace

    var nextTrace = clockClusterizer.getNextTrace(false, Set[Int]())
    stats.record_prune_start
    while (!nextTrace.isEmpty) {
      val ignoredAbsentIndices = new HashSet[Int]
      def ignoreAbsentCallback(idx: Int) {
        ignoredAbsentIndices += idx
      }
      def resetCallback() {
        ignoredAbsentIndices.clear
      }
      val ret = if (testScheduler == TestScheduler.DPORwHeuristics)
        testWithDpor(nextTrace.get, stats, ignoreAbsentCallback, resetCallback)
        else testWithSTSSched(nextTrace.get, stats, ignoreAbsentCallback)

      var ignoredAbsentIds = Set[Int]()
      if (!ret.isEmpty) {
        log.info("Pruning was successful.")
        if (ret.get.size < minTrace.size) {
          minTrace = ret.get
        }
        val adjustedTrace = if (testScheduler == TestScheduler.STSSched)
          nextTrace.get.events
        else nextTrace.get.events.flatMap {
          case u: UniqueMsgEvent => Some(u)
          case _ => None
        }

        ignoredAbsentIndices.foreach { case i =>
          ignoredAbsentIds = ignoredAbsentIds +
            adjustedTrace.get(i).get.asInstanceOf[UniqueMsgEvent].id
        }
      }
      nextTrace = clockClusterizer.getNextTrace(!ret.isEmpty, ignoredAbsentIds)
    }
    stats.record_prune_end

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

// - aggressive: whether to stop trying to remove timers as soon as we've
//   found at least one failing violation for the current cluster.
class ClockClusterizer(
    originalTrace: EventTrace,
    fingerprinter: FingerprintFactory,
    resolutionStrategy: AmbiguityResolutionStrategy,
    aggressive: Boolean=true) {

  val log = LoggerFactory.getLogger("ClockClusterizer")

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

  def getNextTrace(violationReproducedLastRun: Boolean, ignoredAbsentIds: Set[Int])
        : Option[EventTrace] = {
    if (violationReproducedLastRun) {
      timerIterator.producedViolation(currentTimers, ignoredAbsentIds)
      clusterIterator.producedViolation(currentCluster, ignoredAbsentIds)
    }

    if (!timerIterator.hasNext ||
        (aggressive && violationReproducedLastRun && !tryingFirstCluster)) {
      tryingFirstCluster = false
      if (!clusterIterator.hasNext) {
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
