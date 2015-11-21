package akka.dispatch.verification

import scala.collection.mutable.ListBuffer

// A RemovalStrategy that maintains a model of the program's state
// machine, and uses the model to decide which schedules to explore next.
// We currently use Synoptic
// (http://homes.cs.washington.edu/~mernst/pubs/synoptic-fse2011.pdf)
// to build the model from console output of each execution we've tried so far.
class StateMachineRemoval(originalTrace: EventTrace, messageFingerprinter: FingerprintFactory) extends RemovalStrategy {
  // Return how many events we were unwilling to ignore, e.g. because they've
  // been marked by the application as unignorable.
  def unignorable: Int = 0

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
  override def getNextTrace(lastFailingTrace: EventTrace,
                   alreadyRemoved: MultiSet[(String,String,MessageFingerprint)],
                   violationTriggered: Boolean): Option[EventTrace] = {
    return None
  }
}

// Stores all (Meta)EventTraces that have been executed in the past
object HistoricalEventTraces {
  def current: MetaEventTrace = traces.last

  // In order of least recent to most recent
  val traces = new ListBuffer[MetaEventTrace]

  // If you want fast lookup of EventTraces, you could populate a HashMap here:
  // { EventTrace -> MetaEventTrace }
}
