package akka.dispatch.verification

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.util.parsing.json.JSONObject
import scala.util.parsing.json.JSONArray
import scala.util.parsing.json.JSON

trait Minimizer {
  // Returns the MCS.
  def minimize(events: EventDag,
               violation_fingerprint: ViolationFingerprint,
               initializationRoutine: Option[()=>Any]) : EventDag

  def minimize(events: Seq[ExternalEvent],
               violation_fingerprint: ViolationFingerprint,
               initializationRoutine: Option[()=>Any]=None) : EventDag = {
    return minimize(new UnmodifiedEventDag(events), violation_fingerprint,
      initializationRoutine=initializationRoutine)
  }

  // Returns Some(execution) if the final MCS was able to be reproduced,
  // otherwise returns None.
  def verify_mcs(mcs: EventDag,
                 violation_fingerprint: ViolationFingerprint,
                 initializationRoutine: Option[()=>Any]=None) : Option[EventTrace]
}

// Statistics about how the minimization process worked.
class MinimizationStats {
  // minimization_strategy:
  //   One of: {"LeftToRightRemoval", "DDMin"}
  // test_oracle:
  //   One of: {"RandomScheduler", "STSSchedNoPeek", "STSSched",
  //            "DPOR", "FairScheduler", "FungibleClocks"}
  var minimization_strategy : String = ""
  var test_oracle : String = ""
  var stats = new ListBuffer[MinimizationStats.InnerStats]

  // Update which <strategy, oracle> pair we're recording stats for
  def updateStrategy(_minimization_strategy: String, _test_oracle: String) {
    minimization_strategy = _minimization_strategy
    test_oracle = _test_oracle
    val name = ((minimization_strategy,test_oracle)).toString
    stats += new MinimizationStats.InnerStats(name)
  }

  def inner(): MinimizationStats.InnerStats = {
    stats.last
  }

  def reset() {
    inner().reset
  }

  def increment_replays() {
    inner().increment_replays
  }

  def record_replay_start() {
    inner().record_replay_start
  }

  def record_replay_end() {
    inner().record_replay_end
  }

  def record_prune_start() {
    inner().record_prune_start
  }

  def record_prune_end() {
    inner().record_prune_end
  }

  def record_iteration_size(iteration_size: Integer) {
    inner().record_iteration_size(iteration_size)
  }

  def record_distance_increase(newDistance: Integer) {
    inner().record_distance_increase(newDistance)
  }

  def recordDeliveryStats(minimized_deliveries: Int, minimized_externals: Int,
                          minimized_timers: Int) {
    inner().recordDeliveryStats(minimized_deliveries, minimized_externals,
      minimized_timers)
  }

  def toJson(): String = {
    return JSONArray(stats.map(s => s.toJson).toList).toString
  }
}

object MinimizationStats {
  class InnerStats(name: String) {
    // For the ith replay attempt, how many external events were left?
    val iterationSize = new HashMap[Int, Int]
    var iteration = 0
    // For each increase in maxDistance, what was the current iteration when the
    // increase occurred? (See IncrementalDeltaDebugging.scala)
    // { new maxDistance -> current iteration }
    val maxDistance = new HashMap[Int, Int]

    // Number of schedules attempted in order to find the MCS, not including the
    // initial replay to verify the bug is still triggered
    var total_replays = 0

    // Other stats
    val stats = new HashMap[String, Double]

    reset()

    def reset() {
      iterationSize.clear
      maxDistance.clear
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
        "initial_verification_runs_needed" -> 0.0,
        "minimized_deliveries" -> 0.0,
        "minimized_externals" -> 0.0,
        "minimized_timers" -> 0.0
      )
    }

    def increment_replays() {
      total_replays += 1
    }

    def record_replay_start() {
      stats("replay_start_epoch") = System.currentTimeMillis()
    }

    def record_replay_end() {
      stats("replay_end_epoch") = System.currentTimeMillis()
      stats("replay_duration_seconds") =
        (stats("replay_end_epoch") * 1.0 - stats("replay_start_epoch")) / 1000
    }

    def record_prune_start() {
      stats("prune_start_epoch") = System.currentTimeMillis()
    }

    def record_prune_end() {
      stats("prune_end_epoch") = System.currentTimeMillis()
      stats("prune_duration_seconds") =
        (stats("prune_end_epoch") * 1.0 - stats("prune_start_epoch")) / 1000
    }

    def record_iteration_size(iteration_size: Int) {
      iterationSize(iteration) = iteration_size
      iteration += 1
    }

    def recordDeliveryStats(minimized_deliveries: Int, minimized_externals: Int,
                            minimized_timers: Int) {
      stats("minimized_deliveries") = minimized_deliveries / 1.0
      stats("minimized_externals") = minimized_externals / 1.0
      stats("minimized_timers") = minimized_timers / 1.0
    }

    def record_distance_increase(newDistance: Int) {
      maxDistance(newDistance) = iteration
    }

    def toJson(): JSONObject = {
      val map = new HashMap[String, Any]
      map("name") = name
      map("iteration_size") = JSONObject(iterationSize.map(
        pair => pair._1.toString -> pair._2).toMap)
      map("total_replays") = total_replays
      map("maxDistance") = JSONObject(maxDistance.map(
        pair => pair._1.toString -> pair._2).toMap)
      map ++= stats
      return JSONObject(map.toMap)
    }
  }

  def fromJson(json: String): MinimizationStats = {
    val outer = JSON.parseFull(json).get.asInstanceOf[List[Map[String,Any]]]
    val outerObj = new MinimizationStats
    outer.foreach { case inner =>
      val innerObj = new InnerStats(inner("name").asInstanceOf[String])
      innerObj.total_replays = inner("total_replays").asInstanceOf[Double].toInt
      innerObj.stats.keys.foreach { case k =>
        innerObj.stats(k) = inner(k).asInstanceOf[Double] }
      inner("maxDistance").asInstanceOf[Map[String,Any]].foreach { case (k,v) =>
        innerObj.maxDistance(k.toInt) = v.asInstanceOf[Double].toInt }
      inner("iteration_size").asInstanceOf[Map[String,Any]].foreach { case (k,v) =>
        innerObj.iterationSize(k.toInt) = v.asInstanceOf[Double].toInt }
      outerObj.stats += innerObj
    }
    return outerObj
  }
}
