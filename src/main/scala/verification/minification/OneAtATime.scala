package akka.dispatch.verification

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

class LeftToRightRemoval (oracle: TestOracle, checkUnmodifed:Boolean=true,
                          stats: Option[MinimizationStats]=None) extends Minimizer {
  val _stats = stats match {
    case Some(s) => s
    case None => new MinimizationStats
  }
  _stats.updateStrategy("LeftToRightRemoval", oracle.getName)

  val logger = LoggerFactory.getLogger("LeftToRight")

  def minimize(_dag: EventDag,
               violation_fingerprint: ViolationFingerprint,
               initializationRoutine: Option[() => Any]) : EventDag = {
    MessageTypes.sanityCheckTrace(_dag.events)
    // First check if the initial trace violates the exception
    if (checkUnmodifed) {
      logger.info("Checking if unmodified trace triggers violation...")
      if (oracle.test(_dag.events, violation_fingerprint, _stats,
        initializationRoutine=initializationRoutine) == None) {
        throw new IllegalArgumentException("Unmodified trace does not trigger violation")
      }
      _stats.reset
    }

    var dag = _dag
    var events_to_test = dag.get_atomic_events
    var tested_events = new HashSet[AtomicEvent]()

    while (events_to_test.length > 0) {
      // Try removing the event
      val event = events_to_test(0)
      logger.info("Trying removal of event " + event.toString)
      tested_events += event
      val new_dag = dag.remove_events(List(event))

      if (oracle.test(new_dag.get_all_events, violation_fingerprint,
                      _stats, initializationRoutine=initializationRoutine) == None) {
        logger.info("passes")
        // Move on to the next event to test
        events_to_test = events_to_test.slice(1, events_to_test.length)
      } else {
        logger.info("fails. Pruning")
        dag = new_dag
        // The atomic events to test may have changed after removing
        // the event we just pruned, so recompute.
        events_to_test = dag.get_atomic_events.filterNot(e => tested_events.contains(e))
      }
    }

    return dag
  }

  def verify_mcs(mcs: EventDag,
                 violation_fingerprint: ViolationFingerprint,
                 initializationRoutine: Option[() => Any]=None): Option[EventTrace] = {
    val nop_stats = new MinimizationStats
    nop_stats.updateStrategy("NOP", "NOP")
    return oracle.test(mcs.events, violation_fingerprint,
      nop_stats, initializationRoutine=initializationRoutine)
  }
}

