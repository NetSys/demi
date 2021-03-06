package akka.dispatch.verification

// Iteratively choose schedules to explore.
trait RemovalStrategy {
  // Return how many events we were unwilling to ignore, e.g. because they've
  // been marked by the application as unignorable.
  def unignorable: Int

  // Return the next schedule to explore.
  // If there aren't any more schedules to check, return None.
  // Args:
  //   - lastFailingTrace: the most recent schedule we've explored that has successfuly
  //   resulted in the invariant violation.
  //   - alreadyRemoved: any (src,dst,message fingerprint) pairs from the
  //   original schedule that we've already successfully decided aren't
  //   necessary
  //   - violationTriggered: whether the last schedule we returned
  //   successfully triggered the invariant violation, i.e. whether
  //   lastFailingTrace == the most recent trace we returned from getNextTrace.
  def getNextTrace(lastFailingTrace: EventTrace,
                   alreadyRemoved: MultiSet[(String,String,MessageFingerprint)],
                   violationTriggered: Boolean)
                 : Option[EventTrace]
}
