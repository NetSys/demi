package akka.dispatch.verification
// TODO(cs): put me in a different package?

class DDMin (oracle: TestOracle) extends Minimizer {
  // Taken from the 1999 version of delta debugging:
  //   https://www.st.cs.uni-saarland.de/publications/files/zeller-esec-1999.pdf
  // Section 4.
  //
  // Note that this differs from the 2001 version:
  //   https://www.cs.purdue.edu/homes/xyzhang/fall07/Papers/delta-debugging.pdf
  def minimize(events: Seq[ExternalEvent]) : Seq[ExternalEvent] = {
    // First check if the initial trace violates the exception
    println("Checking if unmodified trace triggers violation...")
    if (oracle.test(events)) {
      throw new IllegalArgumentException("Unmodified trace does not trigger violation")
    }

    var dag : EventDag = new UnmodifiedEventDag(events)
    var remainder : EventDag = new UnmodifiedEventDag(List[ExternalEvent]())
    return ddmin2(dag, remainder).get_all_events
  }

  def ddmin2(dag: EventDag, remainder: EventDag) : EventDag = {
    if (dag.get_atomic_events.length <= 1) {
      println("base case")
      return dag
    }

    // N.B. we reverse to ensure that we test the left half of events before
    // the right half of events. Necessary b/c of our use of remove_events().
    // Just a matter of convention.
    val splits : Seq[EventDag] =
        MinificationUtil.split_list(dag.get_atomic_events, 2).
            asInstanceOf[Seq[Seq[AtomicEvent]]].
            map(split => dag.remove_events(split)).reverse

    // First, check both halves.
    for (split <- splits) {
      val union = split.union(remainder)
      println("Checking split")
      val passes = oracle.test(union.get_all_events)
      if (!passes) {
        println("Split fails. Recursing")
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
