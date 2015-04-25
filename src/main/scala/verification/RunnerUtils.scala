package akka.dispatch.verification

import scala.collection.mutable.Queue
import scala.collection.mutable.HashSet

import akka.actor.Props

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge


// Utilities for writing Runner.scala files.
object RunnerUtils {

  def countMsgEvents(trace: Iterable[Event]) : Int = {
    return trace.filter {
      case m: MsgEvent => true
      case t: TimerDelivery => true
      case _ => false
    } size
  }

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
    val origDeliveries = countMsgEvents(traceFound.filterCheckpointMessages.filterFailureDetectorMessages)
    val filtered = provenenceTracker.pruneConcurrentEvents(violationFound)
    val numberFiltered = origDeliveries - countMsgEvents(filtered.map(u => u.event))
    // TODO(cs): track this number somewhere. Or reconstruct it from
    // initialTrace/filtered.
    println("Pruned " + numberFiltered + "/" + origDeliveries + " concurrent deliveries")
    return (traceFound, violationFound, depGraph, initialTrace, filtered)
  }

  def deserializeExperiment(experiment_dir: String,
      messageDeserializer: MessageDeserializer,
      scheduler: ExternalEventInjector[_] with Scheduler,
      traceFile:String=ExperimentSerializer.event_trace):
                  Tuple3[EventTrace, ViolationFingerprint, Option[Graph[Unique, DiEdge]]] = {
    val deserializer = new ExperimentDeserializer(experiment_dir)
    Instrumenter().scheduler = scheduler
    scheduler.populateActorSystem(deserializer.get_actors)
    scheduler.setActorNamePropPairs(deserializer.get_actors)
    val violation = deserializer.get_violation(messageDeserializer)
    val trace = deserializer.get_events(messageDeserializer,
                  Instrumenter().actorSystem, traceFile=traceFile)
    val dep_graph = deserializer.get_dep_graph()
    return (trace, violation, dep_graph)
  }

  def deserializeMCS(experiment_dir: String,
      messageDeserializer: MessageDeserializer,
      scheduler: ExternalEventInjector[_] with Scheduler):
        Tuple4[Seq[ExternalEvent], EventTrace, ViolationFingerprint, Seq[Tuple2[Props, String]]] = {
    val deserializer = new ExperimentDeserializer(experiment_dir)
    Instrumenter().scheduler = scheduler
    scheduler.populateActorSystem(deserializer.get_actors)
    val violation = deserializer.get_violation(messageDeserializer)
    val trace = deserializer.get_events(messageDeserializer, Instrumenter().actorSystem)
    val mcs = deserializer.get_mcs
    val actorNameProps = deserializer.get_actors
    return (mcs, trace, violation, actorNameProps)
  }

  def replayExperiment(experiment_dir: String,
                       fingerprintFactory: FingerprintFactory,
                       messageDeserializer: MessageDeserializer,
                       invariant_check:Option[TestOracle.Invariant],
                       traceFile:String=ExperimentSerializer.event_trace): EventTrace = {
    val replayer = new ReplayScheduler(fingerprintFactory, false, false, invariant_check)
    val (trace, _, _) = RunnerUtils.deserializeExperiment(experiment_dir,
                                        messageDeserializer, replayer,
                                        traceFile=traceFile)

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
      val heuristic = new ArvindDistanceOrdering
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
          val replayTrace = replayer.replay(toReplay)
          replayTrace.setOriginalExternalEvents(mcs)
          verified_mcs = Some(replayTrace)
          println("MCS Validated!")
        } catch {
          case r: ReplayException =>
            println("MCS doesn't reproduce bug... ReplayScheduler")
        } finally {
          replayer.shutdown()
        }
    }
    return (mcs, ddmin.stats, verified_mcs, violation)
  }

  def minimizeInternals(fingerprintFactory: FingerprintFactory,
                        mcs: Seq[ExternalEvent],
                        verified_mcs: EventTrace,
                        actorNameProps: Seq[Tuple2[Props, String]],
                        invariant: TestOracle.Invariant,
                        violation: ViolationFingerprint,
                        event_mapper:Option[HistoricalScheduler.EventMapper]=None) :
      Tuple2[MinimizationStats, EventTrace] = {

    // TODO(cs): factor this out to its own file, with nice interfaces.
    println("Minimizing internals..")
    println("verified_mcs.original_externals: " + verified_mcs.original_externals)
    val stats = new MinimizationStats("InternalMin", "STSSched")

    // TODO(cs): minor optimization: don't try to prune external messages.
    // MsgEvents we've tried ignoring so far. MultiSet to account for duplicate MsgEvent's
    val triedIgnoring = new MultiSet[(String, String, MessageFingerprint)]

    // Filter out the next MsgEvent, and return the resulting EventTrace.
    // If we've tried filtering out all MsgEvents, return None.
    def getNextTrace(trace: EventTrace): Option[EventTrace] = {
      // Track what events we've kept so far in this iteration because we
      // already tried ignoring them previously. MultiSet to account for
      // duplicate MsgEvent's. TODO(cs): this may lead to some ambiguous cases.
      val keysThisIteration = new MultiSet[(String, String, MessageFingerprint)]
      // Whether we've found the event we're going to try ignoring next.
      var foundIgnoredEvent = false

      // Return whether we should keep this event
      def checkDelivery(snd: String, rcv: String, msg: Any): Boolean = {
        val key = (snd, rcv, fingerprintFactory.fingerprint(msg))
        keysThisIteration += key
        if (foundIgnoredEvent) {
          // We already chose our event to ignore. Keep all other events.
          return true
        } else {
          // Check if we should ignore or keep this one.
          if (keysThisIteration.count(key) > triedIgnoring.count(key)) {
            // We found something to ignore
            println("Ignoring next: " + key)
            foundIgnoredEvent = true
            triedIgnoring += key
            return false
          } else {
            // Keep this one; we already tried ignoring it, but it was
            // not prunable.
            return true
          }
        }
      }

      // We accomplish two tasks as we iterate through trace:
      //   - Finding the next event we want to ignore
      //   - Filtering (keeping) everything that we don't want to ignore
      val modified = trace flatMap {
        case m @ MsgEvent(snd, rcv, msg) =>
          if (checkDelivery(snd, rcv, msg)) {
            Some(m)
          } else {
            None
          }
        case t @ TimerDelivery(snd, rcv, msg) =>
          if (checkDelivery(snd, rcv, msg)) {
            Some(t)
          } else {
            None
          }
        case e =>
          Some(e)
      }
      if (foundIgnoredEvent) {
        return Some(new EventTrace(new Queue[Event] ++ modified,
                                   verified_mcs.original_externals))
      }
      // We didn't find anything else to ignore, so we're done
      return None
    }

    val origTrace = verified_mcs.filterCheckpointMessages.filterFailureDetectorMessages
    var lastFailingTrace = origTrace
    // TODO(cs): make this more efficient? Currently O(n^2) overall.
    var nextTrace = getNextTrace(lastFailingTrace)

    while (!nextTrace.isEmpty) {
      val sched = new STSScheduler(nextTrace.get, false, fingerprintFactory, false)
      sched.setActorNamePropPairs(actorNameProps)
      sched.setInvariant(invariant)
      event_mapper match {
        case Some(f) => sched.setEventMapper(f)
        case None => None
      }
      Instrumenter().scheduler = sched
      val prunedOpt = sched.test(mcs, violation, stats)
      prunedOpt match {
        case Some(trace) =>
          // Some other events may have been pruned by virtue of being absent. So
          // we reassign lastFailingTrace, then pick then next trace based on
          // it.
          val filteredTrace = trace.filterCheckpointMessages.filterFailureDetectorMessages
          val origSize = countMsgEvents(lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages)
          val newSize = countMsgEvents(filteredTrace)
          val diff = origSize - newSize
          println("Ignoring worked! Pruned " + diff + "/" + origSize + " deliveries")
          lastFailingTrace = filteredTrace
        case None =>
          // We didn't trigger the violation.
          println("Ignoring didn't work. Trying next")
          None
      }
      nextTrace = getNextTrace(lastFailingTrace)
    }
    val origSize = countMsgEvents(origTrace)
    val newSize = countMsgEvents(lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages)
    val diff = origSize - newSize
    println("Pruned " + diff + "/" + origSize + " deliveries in " +
            stats.total_replays + " replays")
    return (stats, lastFailingTrace)
  }
}
