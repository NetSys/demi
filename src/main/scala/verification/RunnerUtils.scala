package akka.dispatch.verification

import scala.collection.mutable.Queue
import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

import akka.actor.Props

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

import java.io.PrintWriter
import java.io.File


// Example minimization pipeline:
//     fuzz()
//  -> minimizeSendContents
//  -> stsSchedDDmin
//  -> minimizeInternals
//  -> replayExperiment <- loop

// Utilities for writing Runner.scala files.
object RunnerUtils {

  def fuzz(fuzzer: Fuzzer, invariant: TestOracle.Invariant,
           schedulerConfig: SchedulerConfig,
           validate_replay:Option[() => ReplayScheduler]=None,
           invariant_check_interval:Int=30,
           maxMessages:Option[Int]=None,
           randomizationStrategyCtor:() => RandomizationStrategy=() => new FullyRandom,
           computeProvenance:Boolean=true) :
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
      val sched = new RandomScheduler(schedulerConfig, 1,
        invariant_check_interval,
        randomizationStrategy=randomizationStrategyCtor())
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
                if (replayer.violationAtEnd.isEmpty) {
                  deterministic = false
                }
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
    var filtered = new Queue[Unique]
    if (computeProvenance) {
      println("Pruning events not in provenance of violation. This may take awhile...")
      val provenenceTracker = new ProvenanceTracker(initialTrace, depGraph)
      val origDeliveries = countMsgEvents(traceFound.filterCheckpointMessages.filterFailureDetectorMessages)
      filtered = provenenceTracker.pruneConcurrentEvents(violationFound)
      val numberFiltered = origDeliveries - countMsgEvents(filtered.map(u => u.event))
      // TODO(cs): track this number somewhere. Or reconstruct it from
      // initialTrace/filtered.
      println("Pruned " + numberFiltered + "/" + origDeliveries + " concurrent deliveries")
    }
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
                       schedulerConfig: SchedulerConfig,
                       messageDeserializer: MessageDeserializer,
                       traceFile:String=ExperimentSerializer.event_trace): EventTrace = {
    val replayer = new ReplayScheduler(schedulerConfig, false)
    val (trace, _, _) = RunnerUtils.deserializeExperiment(experiment_dir,
                                        messageDeserializer, replayer,
                                        traceFile=traceFile)

    return RunnerUtils.replayExperiment(trace, schedulerConfig,
      Seq.empty, _replayer=Some(replayer))
  }

  def replayExperiment(trace: EventTrace,
                       schedulerConfig: SchedulerConfig,
                       actorNamePropPairs:Seq[Tuple2[Props, String]],
                       _replayer:Option[ReplayScheduler]) : EventTrace = {
    val replayer = _replayer match {
      case Some(replayer) => replayer
      case None =>
        new ReplayScheduler(schedulerConfig, false)
    }
    replayer.setActorNamePropPairs(actorNamePropPairs)
    println("Trying replay:")
    Instrumenter().scheduler = replayer
    val events = replayer.replay(trace)
    println("Done with replay")
    replayer.shutdown
    return events
  }

  def replay(schedulerConfig: SchedulerConfig,
             trace: EventTrace,
             actorNameProps: Seq[Tuple2[Props, String]]) {
    val replayer = new ReplayScheduler(schedulerConfig, false)
    Instrumenter().scheduler = replayer
    replayer.setActorNamePropPairs(actorNameProps)
    println("Trying replay:")
    val events = replayer.replay(trace)
    println("Done with replay")
    replayer.shutdown
  }

  def randomDDMin(experiment_dir: String,
                  schedulerConfig: SchedulerConfig,
                  messageDeserializer: MessageDeserializer) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new RandomScheduler(schedulerConfig, 1, 0)
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
    return (mcs.events, ddmin.stats, validated_mcs, violation)
  }

  def stsSchedDDMin(experiment_dir: String,
                    schedulerConfig: SchedulerConfig,
                    messageDeserializer: MessageDeserializer,
                    allowPeek: Boolean) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new STSScheduler(schedulerConfig, new EventTrace, allowPeek)
    val (trace, violation, _) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, sched)
    sched.original_trace = trace
    stsSchedDDMin(allowPeek, schedulerConfig, trace,
                  violation, _sched=Some(sched))
  }

  def stsSchedDDMin(allowPeek: Boolean,
                    schedulerConfig: SchedulerConfig,
                    trace: EventTrace,
                    violation: ViolationFingerprint,
                    initializationRoutine: Option[() => Any]=None,
                    actorNameProps: Option[Seq[Tuple2[Props, String]]]=None,
                    _sched:Option[STSScheduler]=None,
                    dag: Option[EventDag]=None) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = if (_sched != None) _sched.get else
                new STSScheduler(schedulerConfig, trace, allowPeek)
    Instrumenter().scheduler = sched
    if (actorNameProps != None) {
      sched.setActorNamePropPairs(actorNameProps.get)
    }

    val ddmin = new DDMin(sched)
    val mcs = dag match {
      case Some(d) =>
        ddmin.minimize(d, violation, initializationRoutine)
      case None =>
        // STSSched doesn't actually pay any attention to WaitQuiescence or
        // WaitCondition, so just get rid of them.
        val filteredQuiescence = trace.original_externals flatMap {
          case WaitQuiescence() => None
          case WaitCondition(_) => None
          case e => Some(e)
        }
        ddmin.minimize(filteredQuiescence, violation,
          initializationRoutine=initializationRoutine)
    }
    printMCS(mcs.events)
    println("Validating MCS...")
    var validated_mcs = ddmin.verify_mcs(mcs, violation,
      initializationRoutine=initializationRoutine)
    validated_mcs match {
      case Some(trace) =>
        println("MCS Validated!")
        trace.setOriginalExternalEvents(mcs.events)
        validated_mcs = Some(trace.filterCheckpointMessages)
      case None => println("MCS doesn't reproduce bug...")
    }
    return (mcs.events, ddmin.stats, validated_mcs, violation)
  }

  def roundRobinDDMin(experiment_dir: String,
                      schedulerConfig: SchedulerConfig,
                      messageDeserializer: MessageDeserializer) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new PeekScheduler(schedulerConfig)
    val (trace, violation, _) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, sched)
    sched.setMaxMessages(trace.size)

    // Don't check unmodified execution, since RR will often fail
    val ddmin = new DDMin(sched, false)
    val mcs = ddmin.minimize(trace.original_externals, violation)
    printMCS(mcs.events)
    println("Validating MCS...")
    val validated_mcs = ddmin.verify_mcs(mcs, violation)
    validated_mcs match {
      case Some(_) => println("MCS Validated!")
      case None => println("MCS doesn't reproduce bug...")
    }
    return (mcs.events, ddmin.stats, validated_mcs, violation)
  }

  def editDistanceDporDDMin(experiment_dir: String,
                            schedulerConfig: SchedulerConfig,
                            messageDeserializer: MessageDeserializer,
                            ignoreQuiescence:Boolean=true) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {

    val deserializer = new ExperimentDeserializer(experiment_dir)
    val actorNameProps = deserializer.get_actors

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

    // Sched is just used as a dummy here to deserialize ActorRefs.
    val dummy_sched = new ReplayScheduler(schedulerConfig)
    Instrumenter().scheduler = dummy_sched
    dummy_sched.populateActorSystem(deserializer.get_actors)
    val violation = deserializer.get_violation(messageDeserializer)
    val trace = deserializer.get_events(messageDeserializer,
      Instrumenter().actorSystem).filterFailureDetectorMessages.filterCheckpointMessages

    dummy_sched.shutdown

    return editDistanceDporDDMin(schedulerConfig, trace, actorNameProps,
      depGraph, initialTrace, violation, ignoreQuiescence)
  }

  def editDistanceDporDDMin(schedulerConfig: SchedulerConfig,
                            trace: EventTrace,
                            actorNameProps: Seq[Tuple2[Props, String]],
                            depGraph: Graph[Unique, DiEdge],
                            initialTrace: Queue[Unique],
                            violation: ViolationFingerprint,
                            ignoreQuiescence: Boolean) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {

    def dporConstructor(): DPORwHeuristics = {
      val heuristic = new ArvindDistanceOrdering
      val dpor = new DPORwHeuristics(schedulerConfig,
                          prioritizePendingUponDivergence=true,
                          invariant_check_interval=5,
                          backtrackHeuristic=heuristic)
      dpor.setInitialDepGraph(depGraph)
      dpor.setMaxMessagesToSchedule(initialTrace.size)
      dpor.setInitialTrace(new Queue[Unique] ++ initialTrace)
      dpor.setActorNameProps(actorNameProps)
      heuristic.init(dpor, initialTrace)
      return dpor
    }

    val filtered_externals = DPORwHeuristicsUtil.convertToDPORTrace(trace,
      ignoreQuiescence=ignoreQuiescence)
    val resumableDPOR = new ResumableDPOR(dporConstructor)
    val ddmin = new IncrementalDDMin(resumableDPOR,
                                     checkUnmodifed=true,
                                     stopAtSize=6, maxMaxDistance=8)
    val mcs = ddmin.minimize(filtered_externals, violation)

    // Verify the MCS. First, verify that DPOR can reproduce it.
    // TODO(cs): factor this out.
    println("Validating MCS...")
    var verified_mcs : Option[EventTrace] = None
    val traceOpt = ddmin.verify_mcs(mcs, violation)
    traceOpt match {
      case None =>
        println("MCS doesn't reproduce bug... DPOR")
      case Some(toReplay) =>
        // Now verify that ReplayScheduler can reproduce it.
        println("DPOR reproduced successfully. Now trying ReplayScheduler")
        val replayer = new ReplayScheduler(schedulerConfig, false)
        Instrumenter().scheduler = replayer
        // Clean up after DPOR. Counterintuitively, use Replayer to do this, since
        // DPORwHeuristics doesn't have shutdownSemaphore.
        replayer.shutdown()
        try {
          replayer.populateActorSystem(actorNameProps)
          val replayTrace = replayer.replay(toReplay)
          replayTrace.setOriginalExternalEvents(mcs.events)
          verified_mcs = Some(replayTrace)
          println("MCS Validated!")
        } catch {
          case r: ReplayException =>
            println("MCS doesn't reproduce bug... ReplayScheduler")
        } finally {
          replayer.shutdown()
        }
    }
    return (mcs.events, ddmin.stats, verified_mcs, violation)
  }

  def testWithStsSched(schedulerConfig: SchedulerConfig,
                       mcs: Seq[ExternalEvent],
                       trace: EventTrace,
                       actorNameProps: Seq[Tuple2[Props, String]],
                       violation: ViolationFingerprint,
                       stats: MinimizationStats,
                       initializationRoutine: Option[() => Any]=None,
                       preTest: Option[STSScheduler.PreTestCallback]=None,
                       postTest: Option[STSScheduler.PostTestCallback]=None)
                     : Option[EventTrace] = {
    val sched = new STSScheduler(schedulerConfig, trace, false)
    Instrumenter().scheduler = sched
    sched.setActorNamePropPairs(actorNameProps)
    preTest match {
      case Some(callback) =>
        sched.setPreTestCallback(callback)
      case _ =>
    }
    postTest match {
      case Some(callback) =>
        sched.setPostTestCallback(callback)
      case _ =>
    }
    return sched.test(mcs, violation, stats, initializationRoutine=initializationRoutine)
  }

  // pre: replay(verified_mcs) reproduces the violation.
  def minimizeInternals(schedulerConfig: SchedulerConfig,
                        mcs: Seq[ExternalEvent],
                        verified_mcs: EventTrace,
                        actorNameProps: Seq[Tuple2[Props, String]],
                        violation: ViolationFingerprint,
                        preTest: Option[STSScheduler.PreTestCallback]=None,
                        postTest: Option[STSScheduler.PostTestCallback]=None,
                        initializationRoutine: Option[() => Any]=None) :
      Tuple2[MinimizationStats, EventTrace] = {

    println("Minimizing internals..")
    println("verified_mcs.original_externals: " + verified_mcs.original_externals)
    val removalStrategy = new LeftToRightOneAtATime(verified_mcs, schedulerConfig.messageFingerprinter)
    val minimizer = new STSSchedMinimizer(mcs, verified_mcs, violation,
      removalStrategy, schedulerConfig, actorNameProps,
      initializationRoutine=initializationRoutine,
      preTest=preTest, postTest=postTest)
    return minimizer.minimize()
  }

  // Returns a new MCS, with Send contents shrinked as much as possible.
  // Pre: all Send() events have the same message contents.
  def shrinkSendContents(schedulerConfig: SchedulerConfig,
                         mcs: Seq[ExternalEvent],
                         verified_mcs: EventTrace,
                         actorNameProps: Seq[Tuple2[Props, String]],
                         violation: ViolationFingerprint) : Seq[ExternalEvent] = {
    // Invariants (TODO(cs): specific to akka-raft?):
    //   - Require that all Send() events have the same message contents
    //   - Ensure that whenever a component is masked from one Send()'s
    //     message contents, all other Send()'s have the same component
    //     masked. i.e. throughout minimization, the first invariant holds!
    //   - (Definitely specific to akka-raft:) if an ActorRef component is
    //     removed, also remove the Start event for that node.

    // Pseudocode:
    // for component in firstSend.messageConstructor.getComponents:
    //   if (masking component still triggers bug):
    //     firstSend.maskComponent!

    println("Shrinking Send contents..")
    val stats = new MinimizationStats("ShrinkSendContents", "STSSched")

    val shrinkable_sends = mcs flatMap {
      case s @ Send(dst, ctor) =>
        if (!ctor.getComponents.isEmpty ) {
          Some(s)
        } else {
          None
        }
      case _ => None
    } toSeq

    if (shrinkable_sends.isEmpty) {
      println("No shrinkable sends")
      return mcs
    }

    var components = shrinkable_sends.head.messageCtor.getComponents
    if (shrinkable_sends.forall(s => s.messageCtor.getComponents == components)) {
      println("shrinkable_sends: " + shrinkable_sends)
      throw new IllegalArgumentException("Not all shrinkable_sends the same")
    }

    def modifyMCS(mcs: Seq[ExternalEvent], maskedIndices: Set[Int]): Seq[ExternalEvent] = {
      // Also remove Start() events for masked actors
      val maskedActors = components.zipWithIndex.filter {
        case (e, i) => maskedIndices contains i
      }.map { case (e, i) => e.path.name }.toSet

      return mcs flatMap {
        case s @ Send(dst, ctor) =>
          val updated = Send(dst, ctor.maskComponents(maskedIndices))
          // Be careful to make Send ids the same.
          updated._id = s._id
          Some(updated)
        case s @ Start(_, name) =>
          if (maskedActors contains name) {
            None
          } else {
            Some(s)
          }
        case e => Some(e)
      }
    }

    var maskedIndices = Set[Int]()
    for (i <- 0 until components.size) {
      println("Trying to remove component " + i + ":" + components(i))
      val modifiedMCS = modifyMCS(mcs, maskedIndices + i)
      testWithStsSched(schedulerConfig, modifiedMCS,
                       verified_mcs, actorNameProps, violation, stats) match {
        case Some(trace) =>
          println("Violation reproducable after removing component " + i)
          maskedIndices = maskedIndices + i
        case None =>
          println("Violation not reproducable")
      }
    }

    println("Was able to remove the following components: " + maskedIndices)
    return modifyMCS(mcs, maskedIndices)
  }

  def printMinimizationStats(original_experiment_dir: String,
                             mcs_dir: String,
                             messageDeserializer: MessageDeserializer,
                             schedulerConfig: SchedulerConfig) {
    // Print:
    //  - number of original message deliveries
    //  - deliveries removed by provenance
    //  - number of deliveries pruned by minimizing external events
    //    (including internal deliveries that disappeared "by chance")
    //  - deliveries removed by internal minimization
    //  - final number of deliveries
    // TODO(cs): print how many replays went into each of these steps
    // TODO(cs): number of non-delivery external events that were pruned by minimizing?
    var deserializer = new ExperimentDeserializer(original_experiment_dir)
    val dummy_sched = new ReplayScheduler(schedulerConfig)
    Instrumenter().scheduler = dummy_sched
    dummy_sched.populateActorSystem(deserializer.get_actors)

    val origTrace = deserializer.get_events(messageDeserializer, Instrumenter().actorSystem)
    val provenanceTrace = deserializer.get_filtered_initial_trace()

    deserializer = new ExperimentDeserializer(mcs_dir)

    val mcsTrace = deserializer.get_events(messageDeserializer, Instrumenter().actorSystem)
    val intMinTrace = deserializer.get_events(
          messageDeserializer, Instrumenter().actorSystem,
          traceFile=ExperimentSerializer.minimizedInternalTrace)

    printMinimizationStats(origTrace, provenanceTrace, mcsTrace, intMinTrace,
      schedulerConfig.messageFingerprinter)

    dummy_sched.shutdown
  }

  def printMinimizationStats(
    origTrace: EventTrace, provenanceTrace: Option[Queue[Unique]],
    mcsTrace: EventTrace, intMinTrace: EventTrace, messageFingerprinter: FingerprintFactory) {

    def get_deliveries(trace: EventTrace) : Seq[MsgEvent] = {
      // Make sure not to count checkpoint and failure detector messages.
      // Also filter out Timers from other messages, by ensuring that the
      // "sender" field is "Timer"
      trace.filterFailureDetectorMessages.
            filterCheckpointMessages.flatMap {
        case m @ MsgEvent(s, r, msg) =>
          val fingerprint = messageFingerprinter.fingerprint(msg)
          if ((s == "deadLetters" || s == "Timer") && BaseFingerprinter.isFSMTimer(fingerprint)) {
            Some(MsgEvent("Timer", r, fingerprint))
          } else {
            Some(MsgEvent(s, r, fingerprint))
          }
        case t: TimerDelivery => Some(MsgEvent("Timer", t.receiver, t.fingerprint))
        case e => None
      }.toSeq
    }
    def count_externals(msgEvents: Seq[MsgEvent]): Int = {
      msgEvents flatMap {
        case m @ MsgEvent(_, _, _) =>
          if (EventTypes.isExternal(m)) {
            Some(m)
          } else {
            None
          }
        case _ => None
      } length
    }
    def count_timers(msgEvents: Seq[MsgEvent]): Int = {
      msgEvents flatMap {
        case m @ MsgEvent("Timer", _, _) => Some(m)
        case _ => None
      } length
    }

    val orig_deliveries = get_deliveries(origTrace)
    val orig_externals = count_externals(orig_deliveries)
    val orig_timers = count_timers(orig_deliveries)

    // Assumes get_filtered_initial_trace only contains Unique(MsgEvent)s
    val provenance_deliveries = provenanceTrace match {
      case Some(trace) =>
        trace.flatMap {
          case Unique(m @ MsgEvent(s,r,msg), id) =>
            if (MessageTypes.fromFailureDetector(msg) ||
                MessageTypes.fromCheckpointCollector(msg)) {
              None
            } else if (id == 0) {
              // Filter out root event
              None
            } else {
              val fingerprint = messageFingerprinter.fingerprint(msg)
              if ((s == "deadLetters" || s == "Timer") && BaseFingerprinter.isFSMTimer(fingerprint)) {
                Some(MsgEvent("Timer", r, fingerprint))
              } else {
                Some(MsgEvent(s, r, fingerprint))
              }
            }
          case e => throw new UnsupportedOperationException("Non-MsgEvent:" + e)
        }
      case None =>
        Seq.empty
    }
    val provenance_externals = count_externals(provenance_deliveries)
    val provenance_timers = count_timers(provenance_deliveries)

    // Should be same actors, so no need to populateActorSystem
    val mcs_deliveries = get_deliveries(mcsTrace)
    val mcs_externals = count_externals(mcs_deliveries)
    val mcs_timers = count_timers(mcs_deliveries)

    val intmin_deliveries = get_deliveries(intMinTrace)
    val intmin_externals = count_externals(intmin_deliveries)
    val intmin_timers = count_timers(intmin_deliveries)

    println("Original message deliveries: " + orig_deliveries.size +
            " ("+orig_externals+" externals, "+orig_timers+" timers)")
    if (!provenance_deliveries.isEmpty) {
      println("Removed by provenance: " + (orig_deliveries.size - provenance_deliveries.size) +
              " ("+(orig_externals - provenance_externals)+" externals, "+
              (orig_timers - provenance_timers)+" timers)")
    }
    val ddminPriorDeliveries = if (!provenance_deliveries.isEmpty) provenance_deliveries
                               else orig_deliveries
    val ddminPriorExternals = if (!provenance_deliveries.isEmpty) provenance_externals
                              else orig_externals
    val ddminPriorTimers = if (!provenance_deliveries.isEmpty) provenance_timers
                              else orig_timers

    println("Removed by DDMin: " + (ddminPriorDeliveries.size - mcs_deliveries.size) +
            " ("+(ddminPriorExternals - mcs_externals)+" externals, "+
            (ddminPriorTimers - mcs_timers)+" timers)")
    println("Removed by internal minimization: " + (mcs_deliveries.size - intmin_deliveries.size) +
            " ("+(mcs_externals - intmin_externals)+" externals, "+
            (mcs_timers - intmin_timers)+" timers)")
    println("Final deliveries: " + intmin_deliveries.size +
            " ("+intmin_externals + " externals, "+
            intmin_timers + " timers)")
    println("Final messages delivered:") // w/o fingerints

    // TODO(cs): annotate which events are unignorable.
    RunnerUtils.printDeliveries(intMinTrace)
  }

  def getDeliveries(trace: EventTrace): Seq[Event] = {
    trace flatMap {
      case m: MsgEvent => Some(m)
      case t: TimerDelivery => Some(t)
      case _ => None
    } toSeq
  }

  def getRawDeliveries(trace: EventTrace): Seq[Event] = {
    trace.events flatMap {
      case m: UniqueMsgEvent => Some(m)
      case t: UniqueTimerDelivery => Some(t)
      case _ => None
    } toSeq
  }

  def getFingerprintedDeliveries(trace: EventTrace,
                                 messageFingerprinter: FingerprintFactory):
                               Seq[(String,String,Any)] = {
    RunnerUtils.getDeliveries(trace) map {
      case MsgEvent(snd,rcv,msg) =>
        ((snd,rcv,messageFingerprinter.fingerprint(msg)))
      case TimerDelivery(snd,rcv,f) =>
        ((snd,rcv,f))
    } toSeq
  }

  def countMsgEvents(trace: Iterable[Event]) : Int = {
    return trace.filter {
      case m: MsgEvent => true
      case u: UniqueMsgEvent => true
      case t: TimerDelivery => true
      case u: UniqueTimerDelivery => true
      case _ => false
    } size
  }

  def printDeliveries(trace: EventTrace) {
    getDeliveries(trace) foreach { case e => println(e) }
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

  // Generate a log that can be fed into ShiViz!
  def visualizeDeliveries(trace: EventTrace, outputFile: String) {
    val writer = new PrintWriter(new File(outputFile))
    val logger = new VCLogger(writer)

    val id2delivery = new HashMap[Int, Event]
    trace.events.foreach {
      case u @ UniqueMsgEvent(m, id) =>
        id2delivery(id) = u
      case _ =>
    }

    writer.println("(?<clock>.*\\}) (?<host>[^:]*): (?<event>.*)")
    writer.println("")
    trace.events.foreach {
      case UniqueMsgSend(MsgSend(snd,rcv,msg), id) =>
        if ((id2delivery contains id) && snd != "deadLetters" && snd != "Timer") {
          logger.log(snd, "Sending to " + rcv + ": " + msg)
        }
      case UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) =>
        if (snd == "deadLetters") {
          logger.log(rcv, "Received external message: " + msg)
        } else {
          logger.mergeVectorClocks(snd,rcv)
          logger.log(rcv, "Received message from " + snd + ": " + msg)
        }
      case UniqueTimerDelivery(TimerDelivery(snd,rcv,fingerprint), id) =>
        logger.log(rcv, "Received timer: " + fingerprint)
      case _ =>
    }
    writer.close
    println("ShiViz input at: " + outputFile)
  }

  /**
   * Make it easier to construct specifically delivery orders manually.
   *
   * Given:
   *  - An event trace to be twiddled with
   *  - A sequence of UniqueMsgSends.ids (the id value from the UniqueMsgSends
   *     contained in event trace).
   *
   * Return: a new event trace that has UniqueMsgEvents and
   * UniqueTimerDeliveries arranged in the order
   * specified by the given sequences of ids
   */
  // TODO(cs): allow caller to specify locations of external events. For now
  // just put them all at the front.
  // TODO(cs): use DepGraph ids rather than Unique ids, which are more finiky
  def reorderDeliveries(trace: EventTrace, ids: Seq[Int]): EventTrace = {
    val result = new SynchronizedQueue[Event]

    val id2send = new HashMap[Int, UniqueMsgSend]
    trace.events.foreach {
      case u @ UniqueMsgSend(MsgSend(snd, rcv, msg), id) =>
        if (snd == "Timer") {
          id2send(id) = UniqueMsgSend(MsgSend("deadLetters", rcv, msg), id)
        } else {
          id2send(id) = u
        }
      case _ =>
    }

    val id2delivery = new HashMap[Int, Event]
    trace.events.foreach {
      case u @ UniqueMsgEvent(m, id) =>
        id2delivery(id) = u
      case u @ UniqueTimerDelivery(t, id) =>
        id2delivery(id) = u
      case _ =>
    }

    // Put all SpawnEvents, external events at the beginning of the trace
    trace.events.foreach {
      case u @ UniqueMsgSend(m, id) =>
        if (EventTypes.isExternal(u)) {
          result += u
        }
      case u @ UniqueMsgEvent(m, id) =>
      case u @ UniqueTimerDelivery(t, id) =>
      case e => result += e
    }

    // Now append all the specified deliveries
    ids.foreach {
      case id =>
        if (id2delivery contains id) {
          result += id2delivery(id)
        } else {
          // fabricate a UniqueMsgEvent
          val msgSend = id2send(id).m
          result += UniqueMsgEvent(MsgEvent(
            msgSend.sender, msgSend.receiver, msgSend.msg), id)
        }
    }

    return new EventTrace(result, trace.original_externals)
  }
}
