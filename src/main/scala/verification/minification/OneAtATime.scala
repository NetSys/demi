package akka.dispatch.verification

import scala.collection.mutable.ListBuffer

// TODO(cs): treat failure and recovery events as atomic pairs.

class LeftToRightRemoval (oracle: TestOracle) extends Minimizer {
  def minimize(events: List[ExternalEvent]) : List[ExternalEvent] = {
    var chosen = ListBuffer[ExternalEvent]()
    for ((event,idx) <- events.zipWithIndex) {
      // Try removing the event
      val passes = oracle.test(chosen ++ events.slice(idx + 1, events.length))
      if (passes) {
        chosen = chosen :+ event
      }
    }
    return chosen.toList
  }
}

