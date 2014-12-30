package akka.dispatch.verification

object MinificationUtil {
  def split_list(l: List[Any], split_ways: Int) : List[List[Any]] = {
    // Split our inputs into split_ways separate lists
    if (split_ways < 1) {
      throw new IllegalArgumentException("Split ways must be greater than 0")
    }

    var splits = List[List[Any]]()
    var split_interval = l.length / split_ways // integer division = floor
    var remainder = l.length % split_ways // remainder is guaranteed to be less than splitways

    var start_idx = 0
    while (splits.length < split_ways) {
      var split_idx = start_idx + split_interval
      /*
       * The first 'remainder' chunks are made one element larger to chew
       * up the remaining elements (remainder < splitways).
       * note: len(l) = split_ways *  split_interval + remainder
       */
      if (remainder > 0) {
        split_idx += 1
        remainder -= 1
      }

      val slice = l.slice(start_idx, split_idx)
      splits = splits :+ slice
      start_idx = split_idx
    }
    return splits
  }
}

class EventDag() {
  // Keep failure and recovery events together as an atomic pair.
  def insert_atomic_inputs(events: List[ExternalEvent]) {
  }
}

