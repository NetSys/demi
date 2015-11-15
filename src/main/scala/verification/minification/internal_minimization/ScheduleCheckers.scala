package akka.dispatch.verification

import akka.actor.Props

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

// Minimizes internal events. One-time-use -- you shouldn't invoke minimize() more
// than once.
trait InternalEventMinimizer {
  def minimize(): Tuple2[MinimizationStats, EventTrace]
}

// TODO(cs): we should try supporting DPOR removal of internals.

// Given a RemovalStrategy, checks each schedule returned by the
// RemovalStrategy with the STSSched scheduler.
class STSSchedMinimizer(
  mcs: Seq[ExternalEvent],
  verified_mcs: EventTrace,
  violation: ViolationFingerprint,
  removalStrategy: RemovalStrategy,
  schedulerConfig: SchedulerConfig,
  actorNameProps: Seq[Tuple2[Props, String]],
  initializationRoutine: Option[() => Any]=None,
  preTest: Option[STSScheduler.PreTestCallback]=None,
  postTest: Option[STSScheduler.PostTestCallback]=None,
  stats: Option[MinimizationStats]=None)
  extends InternalEventMinimizer {

  val logger = LoggerFactory.getLogger("IntMin")

  def minimize(): Tuple2[MinimizationStats, EventTrace] = {
    val _stats = stats match {
      case Some(s) => s
      case None => new MinimizationStats
    }
    _stats.updateStrategy("InternalMin", "STSSched")

    val origTrace = verified_mcs.filterCheckpointMessages.filterFailureDetectorMessages
    var lastFailingTrace = origTrace
    var lastFailingSize = RunnerUtils.countMsgEvents(lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages)
    // { (snd,rcv,fingerprint) }
    val prunedOverall = new MultiSet[(String,String,MessageFingerprint)]
    // TODO(cs): make this more efficient? Currently O(n^2) overall.
    var violationTriggered = false
    var nextTrace = removalStrategy.getNextTrace(lastFailingTrace,
      prunedOverall, violationTriggered)

    _stats.record_prune_start
    while (!nextTrace.isEmpty) {
      RunnerUtils.testWithStsSched(schedulerConfig, mcs, nextTrace.get, actorNameProps,
                       violation, _stats, initializationRoutine=initializationRoutine,
                       preTest=preTest, postTest=postTest) match {
        case Some(trace) =>
          // Some other events may have been pruned by virtue of being absent. So
          // we reassign lastFailingTrace, then pick then next trace based on
          // it.
          violationTriggered = true
          val filteredTrace = trace.filterCheckpointMessages.filterFailureDetectorMessages
          val origSize = RunnerUtils.countMsgEvents(lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages)
          val newSize = RunnerUtils.countMsgEvents(filteredTrace)
          val diff = origSize - newSize
          logger.info("Ignoring worked! Pruned " + diff + "/" + origSize + " deliveries")

          val priorDeliveries = new MultiSet[(String,String,MessageFingerprint)]
          priorDeliveries ++= RunnerUtils.getFingerprintedDeliveries(
            lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages,
            schedulerConfig.messageFingerprinter
          )

          val newDeliveries = new MultiSet[(String,String,MessageFingerprint)]
          newDeliveries ++= RunnerUtils.getFingerprintedDeliveries(
            filteredTrace,
            schedulerConfig.messageFingerprinter
          )

          val prunedThisRun = priorDeliveries.setDifference(newDeliveries)
          logger.debug("Pruned: [ignore TimerDeliveries] ")
          prunedThisRun.foreach { case e => logger.debug("" + e) }
          logger.debug("---")

          prunedOverall ++= prunedThisRun

          lastFailingTrace = filteredTrace
          lastFailingTrace.setOriginalExternalEvents(mcs)
          lastFailingSize = newSize
          _stats.record_internal_size(lastFailingSize)
        case None =>
          // We didn't trigger the violation.
          violationTriggered = false
          _stats.record_internal_size(lastFailingSize)
          logger.info("Ignoring didn't work.")
          None
      }
      nextTrace = removalStrategy.getNextTrace(lastFailingTrace,
        prunedOverall, violationTriggered)
    }
    _stats.record_prune_end
    val origSize = RunnerUtils.countMsgEvents(origTrace)
    val newSize = RunnerUtils.countMsgEvents(lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages)
    val diff = origSize - newSize
    logger.info("Pruned " + diff + "/" + origSize + " deliveries (" + removalStrategy.unignorable + " unignorable)" +
            " in " + _stats.inner().total_replays + " replays")
    return (_stats, lastFailingTrace.filterCheckpointMessages.filterFailureDetectorMessages)
  }
}
