package akka.dispatch.verification

import scala.collection.mutable.HashMap

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

  def minimize(events: Seq[ExternalEvent], violation_fingerprint: ViolationFingerprint) : Seq[ExternalEvent] = {
    var currentDistance = 0
    oracle.setMaxDistance(currentDistance)

    // First check if the initial trace violates the exception
    if (checkUnmodifed) {
      println("Checking if unmodified trace triggers violation...")
      if (oracle.test(events, violation_fingerprint, stats) == None) {
        throw new IllegalArgumentException("Unmodified trace does not trigger violation")
      }
    }
    stats.reset()

    var currentMCS = events

    stats.record_prune_start()

    while (currentDistance < maxMaxDistance && currentMCS.size > stopAtSize) {
      println("Trying currentDistance="+currentDistance)
      ddmin = new DDMin(oracle, checkUnmodifed=false)
      currentMCS = ddmin.minimize(currentMCS, violation_fingerprint)
      RunnerUtils.printMCS(currentMCS)
      mergeStats(ddmin.stats)
      currentDistance = if (currentDistance == 0) 2 else currentDistance << 1
      oracle.setMaxDistance(currentDistance)
      stats.record_distance_increase(currentDistance)
    }

    stats.record_prune_end()

    return currentMCS
  }

  def verify_mcs(mcs: Seq[ExternalEvent], _violation_fingerprint: ViolationFingerprint): Option[EventTrace] = {
    return oracle.test(mcs, _violation_fingerprint, new MinimizationStats("NOP", "NOP"))
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
  // subsequence is codified as an immutable string.
  val subseqToDPOR = new HashMap[String, DPORwHeuristics]
  var invariant : Invariant = null
  var currentMaxDistance : Int = 0

  def getName: String = "DPOR"

  def getFingerprintForSubseq(subseq: Seq[ExternalEvent]): String {
    return subseq.map(e => e.label).mkString(",")
  }

  def setInvariant(inv: Invariant) = {
    invariant = inv
  }

  def setMaxDistance(dist: Int) {
    currentMaxDistance = dist
  }

  def test(events: Seq[ExternalEvent],
           violation_fingerprint: ViolationFingerprint,
           stats: MinimizationStats) : Option[EventTrace] = {
    val subseqFingerprint = getFingerprintForSubseq(events)
    if (!(subseqToDPOR contains subseqFingerprint)) {
      subseqToDPOR(subseqFingerprint) = ctor()
      subseqToDPOR(subseqFingerprint).setInvariant(invariant)
    }
    val dpor = subseqToDPOR(subseqFingerprint)
    dpor.setMaxDistance(currentMaxDistance)
    return dpor.test(events, violation_fingerprint, stats)
  }
}
