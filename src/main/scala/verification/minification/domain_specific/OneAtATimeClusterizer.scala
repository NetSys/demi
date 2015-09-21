package akka.dispatch.verification

import scala.collection.mutable.SynchronizedQueue

import scala.reflect.ClassTag

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

// TODO(cs): move me out of domain_specific/.

class SingletonClusterizer(
    originalTrace: EventTrace,
    fingerprinter: FingerprintFactory,
    resolutionStrategy: AmbiguityResolutionStrategy) extends Clusterizer {

  val log = LoggerFactory.getLogger("SingletonClusterizer")

  var sortedIds = getIdsToRemove
  val allIds = sortedIds.toSet
  var successfullyRemoved = Set[Int]()
  var ignoredLastRun = -1

  def getIdsToRemove(): Seq[Int] = {
    var inUnignorableBlock = false
    originalTrace.flatMap {
      case BeginUnignorableEvents =>
        inUnignorableBlock = true
        None
      case EndUnignorableEvents =>
        inUnignorableBlock = false
        None
      case UniqueMsgEvent(m, id)
        if (!EventTypes.isExternal(m) && !inUnignorableBlock) =>
        Some(id)
      case t @ UniqueTimerDelivery(_, _) =>
        throw new IllegalArgumentException("TimerDelivery not supported. Replay first")
      case e => None
    }.toSeq.sorted // Should already be sorted?
  }

  // Iteration:
  // - Pick an event to remove
  // - Wildcard all subsequent events

  def approximateIterations: Int = {
    return allIds.size
  }

  def getNextTrace(violationReproducedLastRun: Boolean, ignoredAbsentIds: Set[Int])
        : Option[EventTrace] = {
    if (sortedIds.isEmpty) {
      return None
    }

    if (violationReproducedLastRun) {
      successfullyRemoved = (successfullyRemoved | ignoredAbsentIds) + ignoredLastRun
    }

    ignoredLastRun = sortedIds.head
    sortedIds = sortedIds.tail

    val currentCluster = allIds -- (successfullyRemoved - ignoredLastRun)

    // TODO(cs): wildly redundant with FungibleClockClustering.scala
    val events = new SynchronizedQueue[Event]
    events ++= originalTrace.events.flatMap {
      case u @ UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) if EventTypes.isExternal(u) =>
        Some(u)
      case UniqueMsgEvent(MsgEvent(snd,rcv,msg), id) =>
        if (currentCluster contains id) {
          val classTag = ClassTag(msg.getClass)
          def messageFilter(pendingMsg: Any): Boolean = {
            ClassTag(pendingMsg.getClass) == classTag
          }
          // Choose the least recently sent message for now.
          Some(UniqueMsgEvent(MsgEvent(snd,rcv,
            WildCardMatch((lst, backtrackSetter) =>
             resolutionStrategy.resolve(messageFilter, lst, backtrackSetter),
             name=classTag.toString)), id))
        } else {
          None
        }
      case _: MsgEvent =>
        throw new IllegalArgumentException("Must be UniqueMsgEvent")
      case t @ UniqueTimerDelivery(_, _) =>
        throw new IllegalArgumentException("TimerDelivery not supported. Replay first")
      case e => Some(e)
    }
    return Some(new EventTrace(events, originalTrace.original_externals))
  }
}
