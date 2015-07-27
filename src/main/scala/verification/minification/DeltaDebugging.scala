package akka.dispatch.verification

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

class DDMin (oracle: TestOracle, checkUnmodifed: Boolean) extends Minimizer {
  def this(oracle: TestOracle) = this(oracle, true)

  val logger = LoggerFactory.getLogger("DDMin")

  var violation_fingerprint : ViolationFingerprint = null
  val stats = new MinimizationStats("DDMin", oracle.getName)
  var initializationRoutine : Option[() => Any] = None
  var original_num_events = 0
  var total_inputs_pruned = 0

  // Taken from the 1999 version of delta debugging:
  //   https://www.st.cs.uni-saarland.de/publications/files/zeller-esec-1999.pdf
  // Section 4.
  //
  // Note that this differs from the 2001 version:
  //   https://www.cs.purdue.edu/homes/xyzhang/fall07/Papers/delta-debugging.pdf
  def minimize(dag: EventDag,
               _violation_fingerprint: ViolationFingerprint,
               _initializationRoutine: Option[() => Any]) : EventDag = {
    MessageTypes.sanityCheckTrace(dag.events)
    violation_fingerprint = _violation_fingerprint
    initializationRoutine = _initializationRoutine

    if (logger.isTraceEnabled()) {
      logger.trace("Minimizing:---")
      dag.events foreach { case e => logger.trace(e.asInstanceOf[UniqueExternalEvent].toStringWithId) }
      logger.trace("---")
    }

    // First check if the initial trace violates the exception
    if (checkUnmodifed) {
      println("Checking if unmodified trace triggers violation...")
      if (oracle.test(dag.events, violation_fingerprint, stats,
                      initializationRoutine=initializationRoutine) == None) {
        throw new IllegalArgumentException("Unmodified trace does not trigger violation")
      }
    }
    stats.reset()

    original_num_events = dag.length
    var remainder : EventDag = new UnmodifiedEventDag(List[ExternalEvent]())

    stats.record_prune_start()
    val mcs_dag = ddmin2(dag, remainder)
    val mcs = mcs_dag.get_all_events
    stats.record_prune_end()

    assert(original_num_events - total_inputs_pruned == mcs.length)
    // Record the final iteration (fencepost)
    stats.record_iteration_size(original_num_events - total_inputs_pruned)
    return mcs_dag
  }

  def verify_mcs(mcs: EventDag,
      _violation_fingerprint: ViolationFingerprint,
      initializationRoutine: Option[() => Any]=None): Option[EventTrace] = {
    return oracle.test(mcs.events, _violation_fingerprint,
      new MinimizationStats("NOP", "NOP"), initializationRoutine=initializationRoutine)
  }

  def ddmin2(dag: EventDag, remainder: EventDag): EventDag = {
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
      println("Checking split " + union.get_all_events.map(e => e.label).mkString(","))
      val passes = oracle.test(union.get_all_events, violation_fingerprint,
        stats, initializationRoutine=initializationRoutine) == None
      // There may have been many replays since the last time we recorded
      // iteration size; record each one's iteration size from before we invoked test()
      for (i <- (stats.iteration until stats.total_replays)) {
        stats.record_iteration_size(original_num_events - total_inputs_pruned)
      }
      if (!passes) {
        println("Split fails. Recursing")
        total_inputs_pruned += (dag.length - split.length)
        return ddmin2(split, remainder)
      } else {
        println("Split passes.")
      }
    }

    // Interference:
    println("Interference")
    val left = ddmin2(splits(0), splits(1).union(remainder))
    val right = ddmin2(splits(1), splits(0).union(remainder))
    return left.union(right)
  }
}
