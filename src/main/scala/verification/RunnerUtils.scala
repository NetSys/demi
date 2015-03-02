package akka.dispatch.verification


object RunnerUtils {

  def fuzz(fuzzer: Fuzzer, invariant: TestOracle.Invariant, shutdownCallback: () => Any) :
        Tuple2[ViolationFingerprint, EventTrace] = {
    var violationFound : ViolationFingerprint = null
    var traceFound : EventTrace = null
    while (violationFound == null) {
      val fuzzTest = fuzzer.generateFuzzTest()
      println("Trying: " + fuzzTest)

      val sched = new RandomScheduler(1, false, 30, false)
      sched.setInvariant(invariant)
      Instrumenter().scheduler = sched
      sched.explore(fuzzTest) match {
        case None =>
          println("Returned to main with events")
          sched.shutdown()
          println("shutdown successfully")
          // raftChecks = new RaftChecks
          shutdownCallback()
        case Some((trace, violation)) => {
          println("Found a safety violation!")
          violationFound = violation
          traceFound = trace
          sched.shutdown()
        }
      }
    }

    return (violationFound, traceFound)
  }

  def replayExperiment(experiment_dir: String, messageFingerprinter: MessageFingerprinter,
                       messageDeserializer: MessageDeserializer) : EventTrace = {
    val deserializer = new ExperimentDeserializer(experiment_dir)
    val replayer = new ReplayScheduler(messageFingerprinter, false, false)
    Instrumenter().scheduler = replayer
    replayer.populateActorSystem(deserializer.get_actors)
    val trace = deserializer.get_events(messageDeserializer, Instrumenter().actorSystem)
    // val violation = deserializer.get_violation(messageDeserializer)

    println("Trying replay:")
    val events = replayer.replay(trace, populateActors=false)
    println("Done with replay")
    replayer.shutdown
    return events
  }
}
