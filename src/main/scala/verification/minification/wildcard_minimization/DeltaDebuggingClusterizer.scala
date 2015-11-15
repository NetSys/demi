package akka.dispatch.verification

/*
import scala.collection.mutable.Stack

class DDMinClusterizer(
    originalTrace: EventTrace,
    fingerprinter: FingerprintFactory,
    resolutionStrategy: AmbiguityResolutionStrategy) {

  val log = LoggerFactory.getLogger("DDMinClusterizer")

  val sortedIds = getIdsToRemove
  val allIds = idsToRemove.toSet

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

  // ids to keep next, remainder
  val callStack = new Stack[(Seq[Int], Set[Int])]
  callStack.push((sortedIds, Set.empty))

  // Iteration:
  // - Pick a subsequence to remove
  // - Wildcard all subsequent events

  def approximateIterations: Int = {
    return idsToRemove.size
  }

  def getNextTrace(violationReproducedLastRun: Boolean, ignoredAbsentIds: Set[Int])
        : Option[EventTrace] = {
    if (callStack.isEmpty) {
      return None
    }

    var (idsToKeep, remainder) = callStack.pop

    // TODO(cs): not correct probably.
    // If base case: roll up the stack!
    while (idsToKeep.size <= 1 && !callStack.isEmpty) {
      var (idsToKeep, remainder) = callStack.pop
    }

    if (callStack.isEmpty) {
      return None
    }

    if (violationReproducedLastRun) {
      // TODO(cs): do something with ignoredAbsentIds
    } else {
      // Interference.
    }

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

    if (dag.get_atomic_events.length <= 1) {
      logger.info("base case")
      return dag
    }

    // N.B. we reverse to ensure that we test the left half of events before
    // the right half of events. (b/c of our use of remove_events())
    // Just a matter of convention.
    val splits : Seq[EventDag] =
        MinificationUtil.split_list(dag.get_atomic_events, 2).
            asInstanceOf[Seq[Seq[AtomicEvent]]].
            map(split => dag.remove_events(split)).reverse

    // First, check both halves.
    for ((split, i) <- splits.zipWithIndex) {
      val union = split.union(remainder)
      logger.info("Checking split " + union.get_all_events.map(e => e.label).mkString(","))
      val trace = oracle.test(union.get_all_events, violation_fingerprint,
        _stats, initializationRoutine=initializationRoutine)
      val passes = trace == None
      _stats.record_iteration_size(original_num_events - total_inputs_pruned)
      if (!passes) {
        logger.info("Split fails. Recursing")
        total_inputs_pruned += (dag.length - split.length)
        return ddmin2(split, remainder)
      } else {
        logger.info("Split passes.")
      }
    }

    // Interference:
    logger.info("Interference")
    val left = ddmin2(splits(0), splits(1).union(remainder))
    val right = ddmin2(splits(1), splits(0).union(remainder))
    return left.union(right)

    */
