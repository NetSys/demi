package akka.dispatch.verification

import scala.collection.mutable.Queue

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge


// Utilities for writing Runner.scala files.
object RunnerUtils {

  def fuzz(fuzzer: Fuzzer, invariant: TestOracle.Invariant,
           fingerprintFactory: FingerprintFactory,
           validate_replay:Option[() => ReplayScheduler]=None) :
        Tuple4[EventTrace, ViolationFingerprint, Graph[Unique, DiEdge], Queue[Unique]] = {
    var violationFound : ViolationFingerprint = null
    var traceFound : EventTrace = null
    var depGraph : Graph[Unique, DiEdge] = null
    var initialTrace : Queue[Unique] = null
    while (violationFound == null) {
      val fuzzTest = fuzzer.generateFuzzTest()
      println("Trying: " + fuzzTest)

      // TODO(cs): it's possible for RandomScheduler to never terminate
      // (waiting for a WaitQuiescene)
      val sched = new RandomScheduler(1, fingerprintFactory, false, 30, false)
      sched.setInvariant(invariant)
      Instrumenter().scheduler = sched
      sched.explore(fuzzTest) match {
        case None =>
          println("Returned to main with events")
          sched.shutdown()
          println("shutdown successfully")
        case Some((trace, violation)) => {
          println("Found a safety violation!")
          depGraph = sched.depTracker.getGraph
          initialTrace = sched.depTracker.getInitialTrace
          sched.shutdown()
          validate_replay match {
            case Some(replayerCtor) =>
              println("Validating replay")
              val replayer = replayerCtor()
              Instrumenter().scheduler = replayer
              var deterministic = true
              try {
                replayer.replay(trace.filterCheckpointMessages)
              } catch {
                case r: ReplayException =>
                  println("doesn't replay deterministically..." + r)
                  deterministic = false
              } finally {
                replayer.shutdown()
              }
              if (deterministic) {
                violationFound = violation
                traceFound = trace
              }
            case None =>
              violationFound = violation
              traceFound = trace
          }
        }
      }
    }

    return (traceFound, violationFound, depGraph, initialTrace)
  }

  def deserializeExperiment(experiment_dir: String,
      messageDeserializer: MessageDeserializer,
      scheduler: ExternalEventInjector[_] with Scheduler):
                  Tuple3[EventTrace, ViolationFingerprint, Option[Graph[Unique, DiEdge]]] = {
    val deserializer = new ExperimentDeserializer(experiment_dir)
    Instrumenter().scheduler = scheduler
    scheduler.populateActorSystem(deserializer.get_actors)
    val violation = deserializer.get_violation(messageDeserializer)
    val trace = deserializer.get_events(messageDeserializer, Instrumenter().actorSystem)
    val dep_graph = deserializer.get_dep_graph()
    return (trace, violation, dep_graph)
  }

  def replayExperiment(experiment_dir: String,
                       fingerprintFactory: FingerprintFactory,
                       messageDeserializer: MessageDeserializer) : EventTrace = {
    val replayer = new ReplayScheduler(fingerprintFactory, false, false)
    val (trace, _, _) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, replayer)

    println("Trying replay:")
    val events = replayer.replay(trace)
    println("Done with replay")
    replayer.shutdown
    return events
  }

  def randomDDMin(experiment_dir: String,
                  fingerprintFactory: FingerprintFactory,
                  messageDeserializer: MessageDeserializer,
                  invariant: TestOracle.Invariant) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new RandomScheduler(1, fingerprintFactory, false, 0, false)
    sched.setInvariant(invariant)
    val (trace, violation, _) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, sched)
    sched.setMaxMessages(trace.size)

    val ddmin = new DDMin(sched, false)
    val mcs = ddmin.minimize(trace.original_externals, violation)
    println("Validating MCS...")
    val validated_mcs = ddmin.verify_mcs(mcs, violation)
    validated_mcs match {
      case Some(_) => println("MCS Validated!")
      case None => println("MCS doesn't reproduce bug...")
    }
    return (mcs, ddmin.stats, validated_mcs, violation)
  }

  def stsSchedDDMin(experiment_dir: String,
                    fingerprintFactory: FingerprintFactory,
                    messageDeserializer: MessageDeserializer,
                    allowPeek: Boolean,
                    invariant: TestOracle.Invariant,
                    event_mapper: Option[HistoricalScheduler.EventMapper]) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new STSScheduler(new EventTrace, allowPeek,
        fingerprintFactory, false)
    sched.setInvariant(invariant)
    event_mapper match {
      case Some(f) => sched.setEventMapper(f)
      case None => None
    }
    val (trace, violation, _) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, sched)
    sched.original_trace = trace

    val ddmin = new DDMin(sched)
    val mcs = ddmin.minimize(trace.original_externals, violation)
    println("Validating MCS...")
    val validated_mcs = ddmin.verify_mcs(mcs, violation)
    validated_mcs match {
      case Some(_) => println("MCS Validated!")
      case None => println("MCS doesn't reproduce bug...")
    }
    return (mcs, ddmin.stats, validated_mcs, violation)
  }

  def roundRobinDDMin(experiment_dir: String,
                      fingerprintFactory: FingerprintFactory,
                      messageDeserializer: MessageDeserializer,
                      invariant: TestOracle.Invariant) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new PeekScheduler(false)
    sched.setInvariant(invariant)
    val (trace, violation, _) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, sched)
    sched.setMaxMessages(trace.size)

    // Don't check unmodified execution, since RR will often fail
    val ddmin = new DDMin(sched, false)
    val mcs = ddmin.minimize(trace.original_externals, violation)
    println("Validating MCS...")
    val validated_mcs = ddmin.verify_mcs(mcs, violation)
    validated_mcs match {
      case Some(_) => println("MCS Validated!")
      case None => println("MCS doesn't reproduce bug...")
    }
    return (mcs, ddmin.stats, validated_mcs, violation)
  }

  def editDistanceDporDDMin(experiment_dir: String,
                            fingerprintFactory: FingerprintFactory,
                            messageDeserializer: MessageDeserializer,
                            invariant: TestOracle.Invariant) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new DPORwHeuristics(true, fingerprintFactory)
    sched.setInvariant(invariant)
    Instrumenter().scheduler = sched
    val deserializer = new ExperimentDeserializer(experiment_dir)
    sched.setActorNameProps(deserializer.get_actors)
    // Start up actors so we can deserialize ActorRefs
    sched.maybeStartActors()
    val violation = deserializer.get_violation(messageDeserializer)
    val trace = deserializer.get_events(messageDeserializer,
      Instrumenter().actorSystem).filterFailureDetectorMessages.filterCheckpointMessages
    println("---------------")
    println("trace:")
    for (e <- trace) {
      println(e)
    }
    println("---------------")

    val depGraphOpt = deserializer.get_dep_graph()
    var depGraph : Graph[Unique, DiEdge] = null
    depGraphOpt match {
      case Some(graph) =>
        depGraph = graph
        sched.setInitialDepGraph(graph)
        println("---------------")
        println("DepGraph:")
        JavaSerialization.withPrintWriter("/tmp", "dep.dot") { pw =>
          pw.write(Util.getDot(graph))
        }
        println("---------------")
      case None => throw new IllegalArgumentException("Need a DepGraph to run DPORwHeuristics")
    }
    val initialTraceOpt = deserializer.get_initial_trace()
    initialTraceOpt match {
      case Some(initialTrace) =>
        println("----------")
        println("initialTrace")
        for (e <- initialTrace) {
          println(e)
        }
        println("----------")
        sched.setDepthBound(initialTrace.size)
        sched.setInitialTrace(new Queue[Unique] ++ initialTrace)
      case None => throw new IllegalArgumentException("Need initialTrace to run DPORwHeuristics")
    }

    // Convert Trace to a format DPOR will understand. Start by getting a list
    // of all actors, to be used for Kill events.
    var allActors = trace flatMap {
      case SpawnEvent(_,_,name,_) => Some(name)
      case _ => None
    }
    // Verify crash-stop, not crash-recovery
    val allActorsSet = allActors.toSet
    assert(allActors.size == allActorsSet.size)

    val filtered_externals = trace.original_externals flatMap {
      case s: Start => Some(s)
      case s: Send => Some(s)
      case w: WaitQuiescence => Some(w)
      case Kill(name) =>
        Some(NetworkPartition(Set(name), allActorsSet))
      case Partition(a,b) =>
        Some(NetworkPartition(Set(a), Set(b)))
      case UnPartition(a,b) =>
        Some(NetworkUnpartition(Set(a), Set(b)))
      case _ => None
    }

    println("----------")
    println("external events:")
    for (e <- filtered_externals) {
      println(e)
    }
    println("----------")

    // Don't check unmodified execution, since it might take too long
    // TODO(cs): codesign DDMin and DPOR. Or, just invoke DPOR and not DDMin.
    val ddmin = new DDMin(sched, true)
    val mcs = ddmin.minimize(filtered_externals, violation)
    // TODO(cs): write a verify_mcs method that uses Replayer instead of
    // TestOracle.
    val verified_mcs = None
    return (mcs, ddmin.stats, verified_mcs, violation)
  }
}
