package akka.dispatch.verification

import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue

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
                        stopAtSize:Int=1, checkUnmodifed:Boolean=true) extends Minimizer {

  var ddmin = new DDMin(oracle, checkUnmodifed=false)
  val stats = new MinimizationStats("IncDDMin", oracle.getName)

  private[this] def mergeStats(otherStats: MinimizationStats) {
    // Increment keys in otherStats.iterationSize by my current iteration
    for ((k, v) <- otherStats.iterationSize) {
      stats.iterationSize(k + stats.iteration) = v
    }
    // Now increment my own iteration
    stats.iteration += otherStats.iteration
    stats.total_replays += otherStats.total_replays
    // Ignore otherStats.stats
  }

  def minimize(dag: EventDag,
               violation_fingerprint: ViolationFingerprint,
               initializationRoutine: Option[()=>Any]) : EventDag = {
    var currentDistance = 0
    oracle.setMaxDistance(currentDistance)

    // First check if the initial trace violates the exception
    if (checkUnmodifed) {
      println("Checking if unmodified trace triggers violation...")
      if (oracle.test(dag.events, violation_fingerprint, stats) == None) {
        throw new IllegalArgumentException("Unmodified trace does not trigger violation")
      }
    }
    stats.reset()

    var currentMCS = dag

    stats.record_prune_start()

    while (currentDistance < maxMaxDistance && currentMCS.events.size > stopAtSize) {
      println("Trying currentDistance="+currentDistance)
      ddmin = new DDMin(oracle, checkUnmodifed=false)
      currentMCS = ddmin.minimize(currentMCS, violation_fingerprint, initializationRoutine)
      RunnerUtils.printMCS(currentMCS.events)
      mergeStats(ddmin.stats)
      currentDistance = if (currentDistance == 0) 2 else currentDistance << 1
      oracle.setMaxDistance(currentDistance)
      stats.record_distance_increase(currentDistance)
    }

    stats.record_prune_end()

    return currentMCS
  }

  def verify_mcs(mcs: EventDag,
                 _violation_fingerprint: ViolationFingerprint,
                 initializationRoutine: Option[()=>Any]=None): Option[EventTrace] = {
    return oracle.test(mcs.events, _violation_fingerprint, new MinimizationStats("NOP", "NOP"))
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
