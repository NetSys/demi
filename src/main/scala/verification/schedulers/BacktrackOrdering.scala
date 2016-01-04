package akka.dispatch.verification

// A subcomponent of DPOR schedulers.

import akka.actor.Cell,
       akka.actor.ActorSystem,
       akka.actor.ActorRef,
       akka.actor.LocalActorRef,
       akka.actor.ActorRefWithCell,
       akka.actor.Actor,
       akka.actor.PoisonPill,
       akka.actor.Props

import akka.dispatch.Envelope,
       akka.dispatch.MessageQueue,
       akka.dispatch.MessageDispatcher

import scala.collection.concurrent.TrieMap,
       scala.collection.mutable.Queue,
       scala.collection.mutable.HashMap,
       scala.collection.mutable.HashSet,
       scala.collection.mutable.ArrayBuffer,
       scala.collection.mutable.ArraySeq,
       scala.collection.mutable.Stack,
       scala.collection.mutable.PriorityQueue,
       scala.math.Ordering,
       scala.reflect.ClassTag

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import Function.tupled

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

       
import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

trait BacktrackOrdering {
  // The priority function we feed into the priority queue.
  // Higher return values get higher priority.
  def getOrdered(key: DPORwHeuristics.BacktrackKey) : Ordered[DPORwHeuristics.BacktrackKey]

  // If DPORwHeuristics is configured to ignore certain backtrack points, this
  // the integer we return here decides how we decide to ignore. Higher numbers
  // are ignored more quickly than lower numbers.
  def getDistance(key: DPORwHeuristics.BacktrackKey) : Int

  // If a key is no longer needed, give the BacktrackOrdering an opportunity
  // to clear state associated with the key.
  def clearKey(key: DPORwHeuristics.BacktrackKey) {}
}

class DefaultBacktrackOrdering extends BacktrackOrdering {
  // Default: order by depth. Longer depth is given higher priority.
  def getOrdered(tuple: DPORwHeuristics.BacktrackKey) = new Ordered[DPORwHeuristics.BacktrackKey] {
    def compare(other: DPORwHeuristics.BacktrackKey) = tuple._1.compare(other._1)
  }

  // TODO(cs): awkward interface, since distance doesn't really make sense
  // here...
  def getDistance(tuple: DPORwHeuristics.BacktrackKey) : Int = {
    return 0
  }
}

// Combine with: DPORwHeuristics.setMaxDistance(0)
class StopImmediatelyOrdering extends BacktrackOrdering {
  // Default: order by depth. Longer depth is given higher priority.
  def getOrdered(tuple: DPORwHeuristics.BacktrackKey) = new Ordered[DPORwHeuristics.BacktrackKey] {
    def compare(other: DPORwHeuristics.BacktrackKey) = tuple._1.compare(other._1)
  }

  def getDistance(tuple: DPORwHeuristics.BacktrackKey) : Int = {
    return Int.MaxValue
  }
}

// Unlike traditional edit distance:
//  - do not consider any changes to word1, i.e. keep word1 fixed.
//  - do not penalize deletions.
//
// This strategy is simply (i) the number of misordered pairs of events from
// the original sequence, plus (ii) any events in the new sequence that aren't
// in the original.
//
// This has a few nice properties:
//    - It accounts for partial orderings (i.e. concurrent events), in the
//      following sense: rather than simply penalizing all events not in the
//      longest common subsequence (original proposal), it also adds extra
//      penalization to reorderings of events not in the longest common
//      subsequence.
//    - It can be computed online
//    - It's simpler than computing longest common subsequence
class ArvindDistanceOrdering extends BacktrackOrdering {
  var sched : DPORwHeuristics = null
  var originalTrace : Queue[Unique] = null
  var originalIndices = new HashMap[Unique, Int]
  // Store all distances globally, to avoid redundant computation.
  var distances = new HashMap[DPORwHeuristics.BacktrackKey, Int]

  // Need a separate constructor b/c of circular dependence between
  // DPORwHeuristics. Might think about changing the interface.
  def init(_sched: DPORwHeuristics, _originalTrace: Queue[Unique]) {
    sched = _sched
    originalTrace = _originalTrace
    for ((e,i) <- originalTrace.zipWithIndex) {
      originalIndices(e) = i
    }
  }

  private[this] def arvindDistance(tuple: DPORwHeuristics.BacktrackKey) : Int = {
    def getPath() : Seq[Unique] = {
      val commonPrefix = sched.getCommonPrefix(tuple._2._1, tuple._2._1).
                               map(node => node.value)
      val postfix = tuple._3
      return commonPrefix ++ postfix ++ Seq(tuple._2._1, tuple._2._2)
    }

    val path = getPath()
    // TODO(cs): make this computation online rather than offline, i.e. look
    // up the distance for the longest known prefix of this path, then compute
    // the remaining distance for the suffix.
    var distance = 0
    for ((e, i) <- path.zipWithIndex) {
      if (!(originalIndices contains e)) {
        // println "Not in original:", e
        distance += 1
      } else {
        for (pred <- path.slice(0,i)) {
          if ((originalIndices contains pred) &&
              originalIndices(pred) > originalIndices(e)) {
            // println "Reordered: ", pred, e
            distance += 1
          }
        }
      }
    }
    return distance
  }

  private[this] def storeArvindDistance(tuple: DPORwHeuristics.BacktrackKey) : Int = {
    if (distances contains tuple) {
      return distances(tuple)
    }
    distances(tuple) = arvindDistance(tuple)
    return distances(tuple)
  }

  def getOrdered(tuple: DPORwHeuristics.BacktrackKey) = new Ordered[DPORwHeuristics.BacktrackKey] {
    def compare(other: DPORwHeuristics.BacktrackKey) : Int = {
      val myDistance = storeArvindDistance(tuple)
      val otherDistance = storeArvindDistance(other)
      if (otherDistance != myDistance) {
        return myDistance.compare(otherDistance)
      }
      // Break ties based on depth
      return tuple._1.compare(other._1)
    }
  }

  def getDistance(tuple: DPORwHeuristics.BacktrackKey) : Int = {
    return storeArvindDistance(tuple)
  }

  override def clearKey(tuple: DPORwHeuristics.BacktrackKey) = {
    distances -= tuple
  }
}

