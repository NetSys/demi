package akka.dispatch.verification

import scala.collection.mutable.HashMap
import scala.util.parsing.json.JSONObject

trait Minimizer {
  // Returns the MCS.
  def minimize(events: Seq[ExternalEvent], violation_fingerprint: ViolationFingerprint) : Seq[ExternalEvent]
  // Returns Some(execution) if the final MCS was able to be reproduced,
  // otherwise returns None.
  def verify_mcs(mcs: Seq[ExternalEvent], violation_fingerprint: ViolationFingerprint) : Option[EventTrace]
}

// Statistics about how the minimization process worked.
// minimization_strategy: One of: {"LeftToRightRemoval", "DDMin"}
// test_oracle: One of: {"RandomScheduler", "STSSchedNoPeek", "STSSched", "GreedyED", "DPOR", "FairScheduler"}
class MinimizationStats(val minimization_strategy: String, val test_oracle: String) {

  // For the ith replay attempt, how many external events were left?
  val iterationSize = new HashMap[Int, Int]
  var iteration = 0

  // Number of schedules attempted in order to find the MCS, not including the
  // initial replay to verify the bug is still triggered
  var total_replays = 0

  // Other stats
  val stats = new HashMap[String, Double]

  reset()

  def reset() {
    iterationSize.clear
    iteration = 0
    stats.clear
    stats ++= Seq(
      // Overall minimization time
      "prune_duration_seconds" -> -1.0,
      "prune_start_epoch" -> -1.0,
      "prune_end_epoch" -> -1.0,
      // Time for the initial replay to verify the bug is still triggered
      "replay_duration_seconds" -> -1.0,
      "replay_end_epoch" -> -1.0,
      "replay_start_epoch" -> -1.0,
      // How long the original fuzz run took
      // TODO(cs): should be a part of EventTrace?
      "original_duration_seconds" -> -1.0,
      // Number of external events in the unmodified execution
      // TODO(cs): should be a part of EventTrace?
      "total_inputs" -> 0.0,
      // Number of events (both internal and external) in the unmodified execution
      // TODO(cs): should be a part of EventTrace?
      "total_events" -> 0.0,
      // How many times we tried replaying unmodified execution before we were
      // able to reproduce the violation
      "initial_verification_runs_needed" -> 0.0
    )
  }

  def increment_replays() {
    total_replays += 1
  }

  def record_replay_start() {
  }

  def record_replay_end() {
  }

  def record_prune_start() {
    stats("prune_start_epoch") = System.currentTimeMillis()
  }

  def record_prune_end() {
    stats("prune_end_epoch") = System.currentTimeMillis()
    stats("prune_duration_seconds") =
      (stats("prune_end_epoch") * 1.0 - stats("prune_start_epoch")) / 1000
  }

  def record_iteration_size(iteration_size: Integer) {
    iterationSize(iteration) = iteration_size
    iteration += 1
  }

  def toJson(): String = {
    val map = new HashMap[String, Any]
    map("iteration_size") = JSONObject(iterationSize.map(
      pair => pair._1.toString -> pair._2).toMap)
    map("total_replays") = total_replays
    map ++= stats
    val json = JSONObject(map.toMap).toString()
    assert(json != "")
    return json
  }
}
