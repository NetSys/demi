package akka.dispatch.verification

import scala.collection.mutable.Queue

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge


// Utilities for writing Runner.scala files.
object RunnerUtils {

  def fuzz(fuzzer: Fuzzer, invariant: TestOracle.Invariant,
           fingerprintFactory: FingerprintFactory,
           validate_replay:Option[() => ReplayScheduler]=None,
           invariant_check_interval:Int=30,
           maxMessages:Option[Int]=None) :
        Tuple5[EventTrace, ViolationFingerprint, Graph[Unique, DiEdge], Queue[Unique], Queue[Unique]] = {
    var violationFound : ViolationFingerprint = null
    var traceFound : EventTrace = null
    var depGraph : Graph[Unique, DiEdge] = null
    var initialTrace : Queue[Unique] = null
    while (violationFound == null) {
      val fuzzTest = fuzzer.generateFuzzTest()
      println("Trying: " + fuzzTest)

      // TODO(cs): it's possible for RandomScheduler to never terminate
      // (waiting for a WaitQuiescene)
      val sched = new RandomScheduler(1, fingerprintFactory, false, invariant_check_interval, false)
      sched.setInvariant(invariant)
      maxMessages match {
        case Some(max) => sched.setMaxMessages(max)
        case None => None
      }
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

    // Before returning, try to prune events that are concurrent with the violation.
    // TODO(cs): currently only DPORwHeuristics makes use of this
    // optimization...
    println("Pruning events not in provenance of violation. This may take awhile...")
    val provenenceTracker = new ProvenanceTracker(initialTrace, depGraph)
    val filtered = provenenceTracker.pruneConcurrentEvents(violationFound)
    val numberFiltered = initialTrace.size - filtered.size
    println("Pruned " + numberFiltered + "/" + initialTrace.size + " concurrent events")
    return (traceFound, violationFound, depGraph, initialTrace, filtered)
  }

  def deserializeExperiment(experiment_dir: String,
      messageDeserializer: MessageDeserializer,
      scheduler: ExternalEventInjector[_] with Scheduler):
                  Tuple3[EventTrace, ViolationFingerprint, Option[Graph[Unique, DiEdge]]] = {
    val deserializer = new ExperimentDeserializer(experiment_dir)
    Instrumenter().scheduler = scheduler
    scheduler.populateActorSystem(deserializer.get_actors)
    scheduler.setActorNamePropPairs(deserializer.get_actors)
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

  def printMCS(mcs: Seq[ExternalEvent]) {
    println("----------")
    println("MCS: ")
    mcs foreach {
      case s @ Send(rcv, msgCtor) =>
        println(s.label + ":Send("+rcv+","+msgCtor()+")")
      case e => println(e.label + ":"+e.toString)
    }
    println("----------")
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
                    event_mapper: Option[HistoricalScheduler.EventMapper]=None) :
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
    // STSSched doesn't actually pay any attention to WaitQuiescence, so just
    // get rid of them.
    val filteredQuiescence = trace.original_externals flatMap {
      case WaitQuiescence() => None
      case e => Some(e)
    }
    val mcs = ddmin.minimize(filteredQuiescence, violation)
    printMCS(mcs)
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
    printMCS(mcs)
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
                            invariant: TestOracle.Invariant,
                            ignoreQuiescence:Boolean=true,
                            event_mapper:Option[HistoricalScheduler.EventMapper]=None) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {

    val deserializer = new ExperimentDeserializer(experiment_dir)
    val actorsNameProps = deserializer.get_actors

    val depGraphOpt = deserializer.get_dep_graph()
    var depGraph : Graph[Unique, DiEdge] = null
    depGraphOpt match {
      case Some(graph) =>
        depGraph = graph
      case None => throw new IllegalArgumentException("Need a DepGraph to run DPORwHeuristics")
    }
    val initialTraceOpt = deserializer.get_filtered_initial_trace()
    var initialTrace : Queue[Unique] = null
    initialTraceOpt match {
      case Some(_initialTrace) =>
        initialTrace = _initialTrace
      case None => throw new IllegalArgumentException("Need initialTrace to run DPORwHeuristics")
    }

    def dporConstructor(): DPORwHeuristics = {
      val heuristic = new AdditionDistanceOrdering
      val dpor = new DPORwHeuristics(true, fingerprintFactory,
                          prioritizePendingUponDivergence=true,
                          invariant_check_interval=5,
                          backtrackHeuristic=heuristic)
      dpor.setActorNameProps(actorsNameProps)
      dpor.setInitialDepGraph(depGraph)
      dpor.setMaxMessagesToSchedule(initialTrace.size)
      dpor.setInitialTrace(new Queue[Unique] ++ initialTrace)
      heuristic.init(dpor, initialTrace)
      return dpor
    }
    // Sched is just used as a dummy here to deserialize ActorRefs.
    val sched = dporConstructor()
    Instrumenter().scheduler = sched
    // Start up actors so we can deserialize ActorRefs
    sched.maybeStartActors()
    val violation = deserializer.get_violation(messageDeserializer)
    val trace = deserializer.get_events(messageDeserializer,
      Instrumenter().actorSystem).filterFailureDetectorMessages.filterCheckpointMessages

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
      // TODO(cs): DPOR ignores Start after we've invoked setActorNameProps... Ignore them here?
      case s: Start => Some(s)
      case s: Send => Some(s)
      // Convert the following externals into Unique's, since DPORwHeuristics
      // needs ids to match them up correctly.
      case w: WaitQuiescence =>
        if (ignoreQuiescence) {
          None
        } else {
          Some(Unique(w, id=w._id))
        }
      case k @ Kill(name) =>
        Some(Unique(NetworkPartition(Set(name), allActorsSet), id=k._id))
      case p @ Partition(a,b) =>
        Some(Unique(NetworkPartition(Set(a), Set(b)), id=p._id))
      case u @ UnPartition(a,b) =>
        Some(Unique(NetworkUnpartition(Set(a), Set(b)), id=u._id))
      case _ => None
    }

    val resumableDPOR = new ResumableDPOR(dporConstructor)
    resumableDPOR.setInvariant(invariant)
    val ddmin = new IncrementalDDMin(resumableDPOR,
                                     checkUnmodifed=true,
                                     stopAtSize=6, maxMaxDistance=8)
    val mcs = ddmin.minimize(filtered_externals, violation)

    // Verify the MCS. First, verify that DPOR can reproduce it.
    println("Validating MCS...")
    var verified_mcs : Option[EventTrace] = None
    val traceOpt = ddmin.verify_mcs(mcs, violation)
    traceOpt match {
      case None =>
        println("MCS doesn't reproduce bug... DPOR")
      case Some(toReplay) =>
        // Now verify that ReplayScheduler can reproduce it.
        println("DPOR reproduced successfully. Now trying ReplayScheduler")
        val replayer = new ReplayScheduler(fingerprintFactory, false, false)
        event_mapper match {
          case Some(f) => replayer.setEventMapper(f)
          case None => None
        }
        Instrumenter().scheduler = replayer
        // Clean up after DPOR. Counterintuitively, use Replayer to do this, since
        // DPORwHeuristics doesn't have shutdownSemaphore.
        replayer.shutdown()
        try {
          replayer.populateActorSystem(actorsNameProps)
          verified_mcs = Some(replayer.replay(toReplay))
          println("MCS Validated!")
        } catch {
          case r: ReplayException =>
            println("MCS doesn't reproduce bug... ReplayScheduler")
        }
    }
    return (mcs, ddmin.stats, verified_mcs, violation)
  }
}
