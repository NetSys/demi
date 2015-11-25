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

// Which Scheduler to check each schedule with.
object TestScheduler extends Enumeration {
  type TestScheduler = Value
  // Backtracks treated as no-ops.
  val STSSched = Value
  // Supports backtracks
  val DPORwHeuristics = Value
}

// Which RemovalStrategy (Clusterizer) to use to choose which schedules we'll
// try exploring.
object ClusteringStrategy extends Enumeration {
  type ClusteringStrategy = Value
  val ClockClusterizer = Value
  val SingletonClusterizer = Value
  val ClockThenSingleton = Value
}

// Minimizes internal events, by iteratively (i) invoking a Clusterizer to get
// the next schedule to check, and (ii) checking each schedule with the given
// TestScheduler.
//
// The Wildcarding isn't actually specified here -- wildcarding is specified
// by the Clusterizer.
//
// See design doc:
//   https://docs.google.com/document/d/1_EqOkSehZVC7oE2hjV1FxY8jw4mXAlM5ESigQYt4ojg/edit
class WildcardMinimizer(
  schedulerConfig: SchedulerConfig,
  mcs: Seq[ExternalEvent],
  trace: EventTrace,
  actorNameProps: Seq[Tuple2[Props, String]],
  violation: ViolationFingerprint,
  skipClockClusters:Boolean=false, // if true, only explore timers
  resolutionStrategy: AmbiguityResolutionStrategy=null, // if null, use BackTrackStrategy
  testScheduler:TestScheduler.TestScheduler=TestScheduler.STSSched,
  depGraph: Option[Graph[Unique,DiEdge]]=None,
  initializationRoutine: Option[() => Any]=None,
  preTest: Option[STSScheduler.PreTestCallback]=None,
  postTest: Option[STSScheduler.PostTestCallback]=None,
  stats: Option[MinimizationStats]=None,
  clusteringStrategy:ClusteringStrategy.ClusteringStrategy=ClusteringStrategy.ClockThenSingleton,
  timeBudgetSeconds:Long=Long.MaxValue)
  extends InternalEventMinimizer {

  val log = LoggerFactory.getLogger("WildCardMin")

  def testWithDpor(nextTrace: EventTrace, stats: MinimizationStats,
                   absentIgnored: STSScheduler.IgnoreAbsentCallback,
                   resetCallback: DPORwHeuristics.ResetCallback,
                   dporBudgetSeconds: Long): Option[EventTrace] = {
    val uniques = new Queue[Unique] ++ nextTrace.events.flatMap {
      case UniqueMsgEvent(m @ MsgEvent(snd,rcv,msg), id) =>
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
                        stopAfterNextTrace=true,
                        budgetSeconds=dporBudgetSeconds)
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
    val ret = dpor.test(filtered_externals, violation, stats)
    // DPORwHeuristics doesn't play nicely with other schedulers, so we need
    // to clean up after it.
    // Counterintuitively, use a dummy Replayer to do this, since
    // DPORwHeuristics doesn't have shutdownSemaphore.
    val replayer = new ReplayScheduler(schedulerConfig, false)
    Instrumenter().scheduler = replayer
    replayer.shutdown()
    return ret
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
    val _stats = stats match {
      case None => new MinimizationStats
      case Some(s) => s
    }
    val oracleName = if (testScheduler == TestScheduler.STSSched)
      "STSSched" else "DPOR"
    // don't updateStrategy if we're being used as a TestOracle
    if (!skipClockClusters) _stats.updateStrategy("FungibleClockMinimizer", oracleName)

    val aggressiveness = if (skipClockClusters)
      Aggressiveness.STOP_IMMEDIATELY
      else Aggressiveness.ALL_TIMERS_FIRST_ITR

    val _resolutionStrategy = if (resolutionStrategy != null)
        resolutionStrategy
        else new BackTrackStrategy(schedulerConfig.messageFingerprinter)

    val clusterizer = clusteringStrategy match {
      case ClusteringStrategy.ClockClusterizer | ClusteringStrategy.ClockThenSingleton =>
        new ClockClusterizer(trace,
          schedulerConfig.messageFingerprinter,
          _resolutionStrategy,
          skipClockClusters=skipClockClusters,
          aggressiveness=aggressiveness)
      case ClusteringStrategy.SingletonClusterizer =>
        new SingletonClusterizer(trace,
          schedulerConfig.messageFingerprinter, _resolutionStrategy)
    }

    // Each cluster gets timeBudgetSeconds / numberOfClusters seconds
    val dporBudgetSeconds = timeBudgetSeconds / (List(clusterizer.approximateIterations,1).max)
    val tStartSeconds = System.currentTimeMillis / 1000

    var minTrace = doMinimize(clusterizer, dporBudgetSeconds, trace, _stats)

    val timeElapsedSeconds = (System.currentTimeMillis / 1000) - tStartSeconds
    val remainingTimeSeconds = timeBudgetSeconds - timeElapsedSeconds

    if (clusteringStrategy == ClusteringStrategy.ClockThenSingleton &&
        remainingTimeSeconds > 0) {
      log.info("Switching to SingletonClusterizer")
      val singletonClusterizer = new SingletonClusterizer(minTrace,
          schedulerConfig.messageFingerprinter, _resolutionStrategy)
      minTrace = doMinimize(singletonClusterizer, remainingTimeSeconds, minTrace, _stats)
    }

    // don't overwrite prune end if we're being used as a TestOracle
    if (!skipClockClusters) {
      _stats.record_prune_end
      // Fencepost
      _stats.record_internal_size(RunnerUtils.countMsgEvents(minTrace))
    }

    return (_stats, minTrace)
  }

  def doMinimize(clusterizer: Clusterizer, budgetSeconds: Long,
                 startTrace: EventTrace, _stats: MinimizationStats): EventTrace = {
    var minTrace = startTrace
    var nextTrace = clusterizer.getNextTrace(false, Set[Int]())
    // don't overwrite prune start if we're being used as a TestOracle
    if (!skipClockClusters) _stats.record_prune_start
    while (!nextTrace.isEmpty) {
      val ignoredAbsentIndices = new HashSet[Int]
      def ignoreAbsentCallback(idx: Int) {
        ignoredAbsentIndices += idx
      }
      def resetCallback() {
        ignoredAbsentIndices.clear
      }
      val ret = if (testScheduler == TestScheduler.DPORwHeuristics)
        testWithDpor(nextTrace.get, _stats, ignoreAbsentCallback,
          resetCallback, budgetSeconds)
        else testWithSTSSched(nextTrace.get, _stats, ignoreAbsentCallback)

      // Record interation size if we're being used for internal minimization
      if (!skipClockClusters) {
        _stats.record_internal_size(RunnerUtils.countMsgEvents(minTrace))
      }

      var ignoredAbsentIds = Set[Int]()
      if (!ret.isEmpty) {
        log.info("Pruning was successful.")
        if (ret.get.size <= minTrace.size) {
          minTrace = ret.get
        }
        val adjustedTrace = if (testScheduler == TestScheduler.STSSched)
          nextTrace.get.
                    filterFailureDetectorMessages.
                    filterCheckpointMessages.
                    subsequenceIntersection(mcs,
                      filterKnownAbsents=schedulerConfig.filterKnownAbsents).
                    events
        else nextTrace.get.events.flatMap { // DPORwHeuristics
          case u: UniqueMsgEvent => Some(u)
          case _ => None
        }

        ignoredAbsentIndices.foreach { case i =>
          ignoredAbsentIds = ignoredAbsentIds +
            adjustedTrace.get(i).get.asInstanceOf[UniqueMsgEvent].id
        }
      }
      nextTrace = clusterizer.getNextTrace(!ret.isEmpty, ignoredAbsentIds)
    }

    return minTrace
  }
}
