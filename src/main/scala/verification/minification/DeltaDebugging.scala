package akka.dispatch.verification

class DDMin (oracle: TestOracle, checkUnmodifed: Boolean) extends Minimizer {
  def this(oracle: TestOracle) = this(oracle, true)

  var violation_fingerprint : ViolationFingerprint = null
  val stats = new MinimizationStats("DDMin", oracle.getName)
  var original_num_events = 0

  // Taken from the 1999 version of delta debugging:
  //   https://www.st.cs.uni-saarland.de/publications/files/zeller-esec-1999.pdf
  // Section 4.
  //
  // Note that this differs from the 2001 version:
  //   https://www.cs.purdue.edu/homes/xyzhang/fall07/Papers/delta-debugging.pdf
  def minimize(events: Seq[ExternalEvent], _violation_fingerprint: ViolationFingerprint) : Seq[ExternalEvent] = {
    violation_fingerprint = _violation_fingerprint

    // First check if the initial trace violates the exception
    if (checkUnmodifed) {
      println("Checking if unmodified trace triggers violation...")
      if (oracle.test(events, violation_fingerprint, stats) == None) {
        throw new IllegalArgumentException("Unmodified trace does not trigger violation")
      }
    }
    stats.reset()
    original_num_events = events.length

    var dag : EventDag = new UnmodifiedEventDag(events)
    var remainder : EventDag = new UnmodifiedEventDag(List[ExternalEvent]())
    stats.record_prune_start()
    val ret = ddmin2(dag, remainder, 0).get_all_events
    stats.record_prune_end()
    // Make sure to record the final iteration size:
    stats.record_iteration_size(ret.size)
    return ret
  }

  def verify_mcs(mcs: Seq[ExternalEvent], _violation_fingerprint: ViolationFingerprint): Option[EventTrace] = {
    return oracle.test(mcs, _violation_fingerprint, new MinimizationStats("NOP", "NOP"))
  }

  def ddmin2(dag: EventDag, remainder: EventDag, total_inputs_pruned: Int): EventDag = {
    if (dag.get_atomic_events.length <= 1) {
      println("base case")
      return dag
    }

    // N.B. we reverse to ensure that we test the left half of events before
    // the right half of events. (b/c of our use of remove_events())
    // Just a matter of convention.
    val splits : Seq[EventDag] =
        MinificationUtil.split_list(dag.get_atomic_events, 2).
            asInstanceOf[Seq[Seq[AtomicEvent]]].
            map(split => dag.remove_events(split)).reverse

    // First, check both halves.
    for ((split, i) <- splits.zipWithIndex) {
      val union = split.union(remainder)
      println("Checking split")
      val passes = oracle.test(union.get_all_events, violation_fingerprint, stats) == None
      // There may have been many replays; record each one's iteration size
      // from before we invoked test
      for (i <- (stats.iteration until stats.total_replays)) {
        stats.record_iteration_size(original_num_events - total_inputs_pruned)
      }
      if (!passes) {
        println("Split fails. Recursing")
        val new_inputs_pruned = if (i == 0) splits(1).length else splits(0).length
        return ddmin2(split, remainder, total_inputs_pruned + new_inputs_pruned)
      } else {
        println("Split passes.")
      }
    }

    // Interference:
    println("Interference")
    val left = ddmin2(splits(0), splits(1).union(remainder),
                      total_inputs_pruned)
    val right = ddmin2(splits(1), splits(0).union(remainder),
                      total_inputs_pruned)
    return left.union(right)
  }
}
