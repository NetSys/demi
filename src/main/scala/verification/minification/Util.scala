package akka.dispatch.verification

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashSet

object MinificationUtil {
  def split_list(l: Seq[Any], split_ways: Int) : Seq[Seq[Any]] = {
    // Split our inputs into split_ways separate lists
    if (split_ways < 1) {
      throw new IllegalArgumentException("Split ways must be greater than 0")
    }

    var splits = List[Seq[Any]]()
    var split_interval = l.length / split_ways // integer division = floor
    var remainder = l.length % split_ways // remainder is guaranteed to be less than splitways

    var start_idx = 0
    while (splits.length < split_ways) {
      var split_idx = start_idx + split_interval
      /*
       * The first 'remainder' chunks are made one element larger to chew
       * up the remaining elements (remainder < splitways).
       * note: len(l) = split_ways *  split_interval + remainder
       */
      if (remainder > 0) {
        split_idx += 1
        remainder -= 1
      }

      val slice = l.slice(start_idx, split_idx)
      splits = splits :+ slice
      start_idx = split_idx
    }
    return splits
  }
}

/**
 * A collection of events that should be treated as an atomic unit, i.e. one
 * should never be removed without removing the others.
 *
 * Example: a Failure event and its subsequent Recovery event.
 */
class AtomicEvent(varargs: ExternalEvent*) {
  var events = List(varargs: _*)

  override def toString() : String = {
    return events.map(e => e.toString).mkString("-")
  }

  override def equals(other: Any) : Boolean = {
    other match {
      case a: AtomicEvent => return a.events == events
      case _ => return false
    }
  }

  override def hashCode() : Int = {
    return events.hashCode()
  }
}

/**
 * An ordered collection of ExternalEvent objects. EventDags are primarily used to present
 * a view of the underlying events with some subset of the input events pruned
 */
trait EventDag {
  /**
   * Ensure that:
   *  - if a failure or partition is removed, its following recovery is also removed
   *  - if a recovery is removed, its preceding failure or partition is also removed
   *  ? if a Start event is removed, all subsequent Send events destined for
   *    that node are removed. BROKEN, see TODO below.
   */
  def remove_events(events: Seq[AtomicEvent]) : EventDag

  /**
   * Return the union of two EventDags.
   */
  def union(other: EventDag) : EventDag

  /**
   * Return all ExternalEvents that can be removed.
   */
  def get_atomic_events() : Seq[AtomicEvent]

  /**
   * Return all ExternalEvents, with AtomicEvents expanded.
   */
  def get_all_events() : Seq[ExternalEvent]

  /**
   * Return get_all_events().length
   */
  def length: Int
}

// Internal utility methods
object EventDag {
  def remove_events(to_remove: Seq[AtomicEvent], events: Seq[ExternalEvent]) : Seq[ExternalEvent] = {
    val flattened = to_remove.map(atomic => atomic.events).flatten
    var all_removed = Set(flattened: _*)
    assume(flattened.length == all_removed.size)
    return events.filter(e => !(all_removed contains e))

    // TODO(cs): figure out why the below optimization screws Delta Debugging
    // up...
    /*
    // The invariant that failures and recoveries are removed atomically is
    // already ensured by the way AtomicEvents are constructed.
    //
    // All that is left for us to handle is dependencies between Start and Send
    // events.

    // Collect up all remaining events.
    var remaining : Seq[ExternalEvent] = ListBuffer()
    // While we collect remaining events, also ensure that for all removed Start events, we
    // remove all subsequent Sends before the next Start event.
    // We accomplish this by tracking whether we recently passed a removed Start event
    // as we iterate through the list.
    val pending_starts = new HashSet[String]()
    for (event <- events) {
      event match {
        case Start(_, node) => {
          if (all_removed.contains(event)) {
            pending_starts += node
          } else if (pending_starts.contains(node)) {
            // The node has started up again, so stop filtering subsequent
            // Send events.
            pending_starts -= node
          }
        }
        case Send(node, _) => {
          if (pending_starts.contains(node)) {
            all_removed += event
          }
        }
        case _ => None
      }

      if (!all_removed.contains(event)) {
        remaining = remaining :+ event
      }
    }

    assume(remaining.length + all_removed.size == events.length)
    return remaining
    */
  }
}

/**
 * An unmodified EventDag.
 */
class UnmodifiedEventDag(events: Seq[ExternalEvent]) extends EventDag {
  val event_to_idx : Map[ExternalEvent, Int] = Util.map_from_iterable(events.zipWithIndex)

  def remove_events(to_remove: Seq[AtomicEvent]) : EventDag = {
    val remaining = EventDag.remove_events(to_remove, events)
    return new EventDagView(this, remaining)
  }

  def union(other: EventDag) : EventDag = {
    if (other.get_all_events.length != 0) {
      throw new IllegalArgumentException("Unknown events")
    }
    return this
  }

  def get_atomic_events() : Seq[AtomicEvent] = {
    return get_atomic_events(events)
  }

  // Internal API
  def get_atomic_events(given_events: Seq[ExternalEvent]) : Seq[AtomicEvent] = {
    // TODO(cs): memoize this computation?
    // As we iterate through events, check whether the "fingerprint" of the
    // current recovery event matches a "fingerprint" of a preceding failure
    // event. If so, group them together as an Atomic pair.
    //
    // For Partition/UnPartition, the Partition always comes first. For
    // Kill/Start, the Start always comes first.
    var fingerprint_2_previous_dual : Map[String, ExternalEvent] = Map()
    var atomics = new ListBuffer[AtomicEvent]()

    for (event <- given_events) {
      event match {
        case Kill(name) => {
          fingerprint_2_previous_dual get name match {
            case Some(start) => {
              atomics = atomics :+ new AtomicEvent(start, event)
              fingerprint_2_previous_dual -= name
            }
            case None => {
              throw new RuntimeException("Kill without preceding Start")
            }
          }
        }
        case Partition(a,b) => {
          val key = a + "->" + b
          fingerprint_2_previous_dual(key) = event
        }
        case Start(_, name) => {
          fingerprint_2_previous_dual(name) = event
        }
        case UnPartition(a,b) => {
          val key = a + "->" + b
          fingerprint_2_previous_dual get key match {
            case Some(partition) => {
              atomics = atomics :+ new AtomicEvent(partition, event)
              fingerprint_2_previous_dual -= key
            }
            case None => {
              throw new RuntimeException("UnPartition without preceding Partition")
            }
          }
        }
        case _ => atomics = atomics :+ new AtomicEvent(event)
      }
    }

    // Make sure to include any remaining Start or Partition events that never had
    // corresponding Kill/UnPartition events.
    for (e <- fingerprint_2_previous_dual.values) {
      atomics = atomics :+ new AtomicEvent(e)
    }

    assume(atomics.map(atomic => atomic.events).flatten.length == given_events.length)
    // Sort by the original index of first element in the event list.
    return atomics.sortBy[Int](a => event_to_idx(a.events(0)))
  }

  def get_all_events() : Seq[ExternalEvent] = {
    return events
  }

  def length: Int = events.length
}

/**
 * A subsequence of an EventDag.
 */
// All EventDagViews have a pointer to the original UnmodifiedEventDag, so
// that we can compute the proper ordering of events in a union() of two distinct
// EventDagViews.
class EventDagView(parent: UnmodifiedEventDag, events: Seq[ExternalEvent]) extends EventDag {

  def remove_events(to_remove: Seq[AtomicEvent]) : EventDag = {
    val remaining = EventDag.remove_events(to_remove, events)
    return new EventDagView(parent, remaining)
  }

  def union(other: EventDag) : EventDag = {
    // Set isn't strictly necessary, just a sanity check.
    val union = Set((events ++ other.get_all_events): _*).
                     toList.sortBy[Int](e => parent.event_to_idx(e))
    assume(events.length + other.get_all_events.length == union.length)
    return new EventDagView(parent, union)
  }

  def get_atomic_events() : Seq[AtomicEvent] = {
    return parent.get_atomic_events(events)
  }

  def get_all_events() : Seq[ExternalEvent] = {
    return events
  }

  def length: Int = events.length
}
