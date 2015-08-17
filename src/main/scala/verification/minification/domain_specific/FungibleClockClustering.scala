package akka.dispatch.verification

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import akka.actor.Props

import scala.util.Sorting

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge


// Domain-specific strategy:
//  - Cluster messages according to their clock value
//  - For each cluster:
//     - remove the cluster
//     - decrement the clock values of all subsequent clusters
//     - see if the violation is still produced
class FungibleClockMinimizer(
  schedulerConfig: SchedulerConfig,
  mcs: Seq[ExternalEvent],
  trace: EventTrace,
  actorNameProps: Seq[Tuple2[Props, String]],
  violation: ViolationFingerprint,
  stats: MinimizationStats,
  initializationRoutine: Option[() => Any]=None,
  preTest: Option[STSScheduler.PreTestCallback]=None,
  postTest: Option[STSScheduler.PostTestCallback]=None)
  extends InternalEventMinimizer {

  def minimize(): Tuple2[MinimizationStats, EventTrace] = {
    val stats = new MinimizationStats("FungibleClockMinimizer", "STSSched")
    val clockClusterizer = new ClockClusterizer(trace,
      schedulerConfig.messageFingerprinter)
    var nextTrace = clockClusterizer.getNextTrace()
    while (!nextTrace.isEmpty) {
      stats.increment_replays
      val ret = RunnerUtils.testWithStsSched(
        schedulerConfig,
        mcs,
        nextTrace.get,
        actorNameProps,
        violation,
        stats,
        initializationRoutine=initializationRoutine,
        preTest=preTest,
        postTest=postTest)
      println("RET WAS: " + ret.size)
      // TODO(cs): if (!ret.isEmpty)
    }

    return (stats, null)
  }
}

class ClockClusterizer(
    originalTrace: EventTrace,
    fingerprinter: FingerprintFactory) {

  // Clustering:
  // - Cluster all message deliveries according to their Term number.
  // - Find all preceding TimerDeliveries for each cluster, and add them
  //   to the cluster. Unfortunately, there's a caveat: the `generation`
  //   field of the Timers probably means that we can't just move them around
  //   arbitrarily.
  //   [General strategy: we know that the 0th TimerDelivery for a
  //   particular Actor belongs to the cluster with clock=1, 1st TimerDelivery to the
  //   cluster with clock=2 etc. Tell DPOR to deliver N TimerDeliveries of
  //   unspecified generation before the next cluster.]

  // Iteration:
  // - Pick a cluster to remove
  // - Decrement the Term numbers for all subsequent clusters
  // - Always include all other events that do not have a Term number

  var clocksToRemove = new Queue[Long] ++ originalTrace.flatMap {
    case MsgEvent(snd,rcv,msg) =>
      fingerprinter.getLogicalClock(msg)
    case _ => None
  }.toSet.toSeq.sorted

  // TODO(cs): pass in previous clusters we have successfully removed?
  def getNextTrace() : Option[EventTrace] = {
    if (clocksToRemove.isEmpty) {
      return None
    }
    val clockToRemove = clocksToRemove.dequeue
    val actor2clock = new HashMap[String,Long]
    val events = new SynchronizedQueue[Event]
    events ++= originalTrace.flatMap {
      case m @ MsgEvent(snd,rcv,msg) =>
        if (fingerprinter.causesClockIncrement(msg)) {
          val currentClock = actor2clock.getOrElse(rcv, 0.toLong)
          if (currentClock + 1 == clockToRemove) {
            None
          } else {
            actor2clock(rcv) = currentClock + 1
            Some(MsgEvent(snd,rcv,WildCardMatch("deadLetters",
              rcv, fingerprinter.causesClockIncrement)))
          }
        } else {
          val clock = fingerprinter.getLogicalClock(msg)
          if (clock.isEmpty) {
            Some(m)
          } else if (clock.get == clockToRemove) {
            None
          } else if (clock.get > clockToRemove) {
            Some(MsgEvent(snd,rcv,fingerprinter.decrementLogicalClock(clock.get)))
          } else {
            Some(m)
          }
        }
      case e => Some(e)
    }

    return Some(new EventTrace(events, originalTrace.original_externals))
  }
}
