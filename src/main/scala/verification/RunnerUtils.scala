package akka.dispatch.verification

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

// Utilities for writing Runner.scala files.
object RunnerUtils {

  def fuzz(fuzzer: Fuzzer, invariant: TestOracle.Invariant,
           validate_replay:Option[ReplayScheduler]=None) :
        Tuple3[EventTrace, ViolationFingerprint, Graph[Unique, DiEdge]] = {
    var violationFound : ViolationFingerprint = null
    var traceFound : EventTrace = null
    var depGraph : Graph[Unique, DiEdge] = null
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
        case Some((trace, violation)) => {
          println("Found a safety violation!")
          depGraph = sched.depGraph
          sched.shutdown()
          validate_replay match {
            case Some(replayer) =>
              println("Validating replay")
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

    return (traceFound, violationFound, depGraph)
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
                       messageFingerprinter: MessageFingerprinter,
                       messageDeserializer: MessageDeserializer) : EventTrace = {
    val replayer = new ReplayScheduler(messageFingerprinter, false, false)
    val (trace, _, _) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, replayer)

    println("Trying replay:")
    val events = replayer.replay(trace)
    println("Done with replay")
    replayer.shutdown
    return events
  }

  def randomDDMin(experiment_dir: String,
                  messageFingerprinter: MessageFingerprinter,
                  messageDeserializer: MessageDeserializer,
                  invariant: TestOracle.Invariant) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new RandomScheduler(1, false, 0, false)
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
                    messageFingerprinter: MessageFingerprinter,
                    messageDeserializer: MessageDeserializer,
                    allowPeek: Boolean,
                    invariant: TestOracle.Invariant,
                    event_mapper: Option[HistoricalScheduler.EventMapper]) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new STSScheduler(new EventTrace, allowPeek,
        messageFingerprinter, false)
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
                      messageFingerprinter: MessageFingerprinter,
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
                            messageFingerprinter: MessageFingerprinter,
                            messageDeserializer: MessageDeserializer,
                            invariant: TestOracle.Invariant) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new DPORwHeuristics
    Instrumenter().scheduler = sched
    val deserializer = new ExperimentDeserializer(experiment_dir)
    sched.setActorNameProps(deserializer.get_actors)
    val violation = deserializer.get_violation(messageDeserializer)
    val trace = deserializer.get_events(messageDeserializer, Instrumenter().actorSystem)
    val dep_graph = deserializer.get_dep_graph()
    sched.setDepthBound(trace.size)
    sched.setInvariant(invariant)

    // Convert to a format that DPOR will understand
    var allActors = trace flatMap {
      case SpawnEvent(_,_,name,_) => Some(name)
      case _ => None
    }
    // Verify crash-stop, not crash-recovery
    val allActorsSet = allActors.toSet
    assert(allActors.size == allActorsSet.size)

    // DPOR's initialTrace argument contains only Unique SpawnEvents, MsgEvents,
    // NetworkPartitions, and WaitQuiescence events.
    val initialTrace = trace.filterFailureDetectorMessages.filterCheckpointMessages flatMap {
      case s: SpawnEvent =>
        // DPOR ignores Spawns
        None
      case BeginWaitQuiescence =>
        Some(Unique(WaitQuiescence()))
      case m: UniqueMsgEvent =>
        // TODO(cs): verify that timers and Send()s are matched correctly by DPOR
        // TODO(cs): does DPOR assume that system messages all have id=0? see
        // getNextTraceMessage
        Some(Unique(m.m, m.id))
      case KillEvent(actor) =>
        Some(NetworkPartition(Set(actor), allActorsSet))
      case PartitionEvent((a,b)) =>
        None
        // XXX verify no subsequent UnPartitionEvents
        // NetworkPartition(Set(a), Set(b))
      case UnPartitionEvent((a,b)) =>
        None // XXX
      case Quiescence => None
      case ChangeContext(_) => None
      case UniqueMsgSend(_, _) => None
      case TimerSend(_) => None
      case TimerDelivery(_) => None // XXX
    }

    // Don't check unmodified execution, since it might take too long
    // TODO(cs): codesign DDMin and DPOR. Or, just invoke DPOR and not DDMin.
    val ddmin = new DDMin(sched, false)
    // TODO(cs): do we need to specify f1, f2 (args to DPOR.run)?
    val mcs = ddmin.minimize(trace.original_externals, violation)
    // TODO(cs): write a verify_mcs method that uses Replayer instead of
    // TestOracle.
    val verified_mcs = null
    return (mcs, ddmin.stats, verified_mcs, violation)
  }
}
