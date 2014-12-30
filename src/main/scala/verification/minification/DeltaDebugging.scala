package akka.dispatch.verification
// TODO(cs): put me in a different package?
// TODO(cs): treat failure and recovery events as atomic pairs.

class IndexedEvent(_idx: Integer, _event: ExternalEvent) extends Ordered[IndexedEvent] {
  val idx = _idx
  val event = _event

  def compare(that: IndexedEvent) : Int = {
    return this.idx - that.idx
  }
}

class DDMin (oracle: TestOracle) extends Minimizer {
  // Taken from the 1999 version of delta debugging:
  //   https://www.st.cs.uni-saarland.de/publications/files/zeller-esec-1999.pdf
  // Section 4.
  //
  // Note that this differs from the 2001 version:
  //   https://www.cs.purdue.edu/homes/xyzhang/fall07/Papers/delta-debugging.pdf
  def minimize(events: List[ExternalEvent]) : List[ExternalEvent] = {
    var remainder = List[IndexedEvent]()
    var indexed = events.zipWithIndex.map(t => new IndexedEvent(t._2, t._1))
    return ddmin2(indexed, remainder).map(e => e.event)
  }

  def ddmin2(events: List[IndexedEvent],
             remainder: List[IndexedEvent]) : List[IndexedEvent] = {
    if (events.length <= 1) {
      return events
    }

    val splits : List[List[IndexedEvent]] =
        MinificationUtil.split_list(events, 2).asInstanceOf[List[List[IndexedEvent]]]

    // First, check both halves:
    for (split <- splits) {
      // Always sort by index before executing.
      var union = (split ++ remainder).sorted.map(e => e.event)
      var passes = oracle.test(union)
      if (!passes) {
        return ddmin2(split, remainder)
      }
    }

    // Interference:
    var left = ddmin2(splits(0), splits(1) ++ remainder)
    var right = ddmin2(splits(1), splits(0) ++ remainder)
    return left ++ right
  }
}
