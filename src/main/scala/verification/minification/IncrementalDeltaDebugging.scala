package akka.dispatch.verification

import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

/**
 * Invoke DDMin with maxDistance=0, then again with maxDistance=2, ... up to
 * given maxMaxDistance.
 *
 * Depends crucially on the assumption that DPOR tracks its history, so that
 * it doesn't ever explore the same path twice.
 *
 * stopAtSize: if MCS size <= stopAtSize, stop.
 */
// TODO(cs): propogate stopAtSize to DDMin?
class IncrementalDDMin (oracle: ResumableDPOR, maxMaxDistance:Int=256,
                        stopAtSize:Int=1, checkUnmodifed:Boolean=false,
                        stats: Option[MinimizationStats]=None) extends Minimizer {

  val logger = LoggerFactory.getLogger("IncrementalDDMin")
  var ddmin = new DDMin(oracle, checkUnmodifed=false)
  val _stats = stats match {
    case Some(s) => s
    case None => new MinimizationStats
  }
  _stats.updateStrategy("IncDDMin", oracle.getName)

  private[this] def mergeStats(otherStats: MinimizationStats) {
    // Increment keys in otherStats.iterationSize by my current iteration
    for ((k, v) <- otherStats.inner().iterationSize) {
      _stats.inner().iterationSize(k + _stats.inner().iteration) = v
    }
    // Now increment my own iteration
    _stats.inner().iteration += otherStats.inner().iteration
    _stats.inner().total_replays += otherStats.inner().total_replays
    // Ignore otherStats.stats
  }

  def minimize(dag: EventDag,
               violation_fingerprint: ViolationFingerprint,
               initializationRoutine: Option[()=>Any]) : EventDag = {
    var currentDistance = 0
    oracle.setMaxDistance(currentDistance)

    // First check if the initial trace violates the exception
    if (checkUnmodifed) {
      logger.info("Checking if unmodified trace triggers violation...")
      if (oracle.test(dag.events, violation_fingerprint, _stats) == None) {
        throw new IllegalArgumentException("Unmodified trace does not trigger violation")
      }
    }
    _stats.reset()

    var currentMCS = dag

    _stats.record_prune_start()

    while (currentDistance < maxMaxDistance && currentMCS.events.size > stopAtSize) {
      logger.info("Trying currentDistance="+currentDistance)
      ddmin = new DDMin(oracle, checkUnmodifed=false)
      currentMCS = ddmin.minimize(currentMCS, violation_fingerprint, initializationRoutine)
      RunnerUtils.printMCS(currentMCS.events)
      mergeStats(ddmin._stats)
      currentDistance = if (currentDistance == 0) 2 else currentDistance << 1
      oracle.setMaxDistance(currentDistance)
      _stats.record_distance_increase(currentDistance)
    }

    _stats.record_prune_end()

    return currentMCS
  }

  def verify_mcs(mcs: EventDag,
                 _violation_fingerprint: ViolationFingerprint,
                 initializationRoutine: Option[()=>Any]=None): Option[EventTrace] = {
    val nopStats = new MinimizationStats
    nopStats.updateStrategy("NOP", "NOP")
    return oracle.test(mcs.events, _violation_fingerprint, nopStats)
  }
}

object ResumableDPOR {
  type DPORConstructor = () => DPORwHeuristics
}

/**
 * A wrapper class that keeps separate instances of DPOR for each external
 * event subsequence.
 */
class ResumableDPOR(ctor: ResumableDPOR.DPORConstructor) extends TestOracle {
  // External event subsequence -> DPOR instance
  val subseqToDPOR = new HashMap[Seq[ExternalEvent], DPORwHeuristics]
  var invariant : Invariant = null
  var currentMaxDistance : Int = 0

  def getName: String = "DPOR"

  def setInvariant(inv: Invariant) = {
    invariant = inv
  }

  def setMaxDistance(dist: Int) {
    currentMaxDistance = dist
  }

  def test(events: Seq[ExternalEvent],
           violation_fingerprint: ViolationFingerprint,
           stats: MinimizationStats,
           init:Option[()=>Any]=None) : Option[EventTrace] = {
    if (!(subseqToDPOR contains events)) {
      subseqToDPOR(events) = ctor()
      subseqToDPOR(events).setInvariant(invariant)
    }
    val dpor = subseqToDPOR(events)
    dpor.setMaxDistance(currentMaxDistance)
    return dpor.test(events, violation_fingerprint, stats, init)
  }
}
