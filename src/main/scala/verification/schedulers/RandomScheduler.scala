package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable.Set
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Random

// Provides O(1) insert and removeRandomElement
class RandomizedHashSet[E] {
  // We store a counter along with each element E to ensure uniqueness
  var arr = new ArrayBuffer[(E,Int)]
  // Value is index into array
  var hash = new HashMap[(E,Int),Int]
  val rand = new Random(System.currentTimeMillis());

  def insert(value: E) = {
    var uniqueness_counter = 0
    while (hash.contains((value, uniqueness_counter))) {
      uniqueness_counter += 1
    }
    val tuple : (E,Int) = (value,uniqueness_counter)
    val i = arr.length
    hash(tuple) = i
    arr += tuple
  }

  private[this] def remove(value: (E,Int)) = {
    // We are going to replace the cell that contains value in A with the last
    // element in A. let d be the last element in the array A at index m. let
    // i be H[value], the index in the array of the value to be removed. Set
    // A[i]=d, H[d]=i, decrease the size of the array by one, and remove value
    // from H.
    if (!hash.contains(value)) {
      throw new IllegalArgumentException("Value " + value + " does not exist")
    }
    val i = hash(value)
    val m = arr.length - 1
    val d = arr(m)
    arr(i) = d
    hash(d) = i
    arr = arr.dropRight(1)
    hash -= value
  }

  def removeRandomElement () : E = {
    val random_idx = rand.nextInt(arr.length)
    val v = arr(random_idx)
    remove(v)
    return v._1
  }

  def isEmpty () : Boolean = {
    return arr.isEmpty
  }
}


/**
 * Takes a list of ExternalEvents as input, and explores random interleavings
 * of internal messages until either a maximum number of interleavings is
 * reached, or a given invariant is violated.
 *
 * Additionally records internal and external events that occur during
 * executions that trigger violations.
 */
class RandomScheduler(max_interleavings: Int)
    extends AbstractScheduler with ExternalEventInjector with TestOracle {

  var test_invariant : Invariant = null

  // Current set of enabled events.
  // First element of tuple is the receiver
  var pendingInternalEvents = new RandomizedHashSet[(ActorCell, Envelope)]

  // Current set of externally injected events, to be delivered in the order
  // they arrive.
  var pendingExternalEvents = new Queue[(ActorCell, Envelope)]

  /**
   * Given an external event trace, randomly explore executions involving those
   * external events.
   *
   * Returns a trace of the internal and external events observed if a failing execution was found,
   * otherwise returns null if no failure was triggered within max_interleavings.
   *
   * Precondition: setInvariant has been invoked.
   */
  def explore (_trace: Seq[ExternalEvent]) : Seq[Event] = {
    for (i <- 1 to max_interleavings) {
      println("Trying random interleaving " + i)
      execute_trace(_trace)

      // Check the invariant at the end of the trace.
      if (test_invariant == null) {
        throw new IllegalArgumentException("Must invoke setInvariant before test()")
      }
      val passes = test_invariant(_trace)
      if (!passes) {
        println("Found failing execution")
        return event_orchestrator.events
      } else if (i != max_interleavings) {
        // 'Tis a lesson you should heed: Try, try, try again.
        // If at first you don't succeed: Try, try, try again
        reset_all_state
      }
    }
    // No bug found...
    return null
  }

  override def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    handle_event_produced(snd, rcv, envelope) match {
      case InternalMessage() => {
        if (!crosses_partition(snd, rcv)) {
          pendingInternalEvents.insert((cell, envelope))
        }
      }
      case ExternalMessage() => {
        pendingExternalEvents += ((cell, envelope))
      }
      case SystemMessage() => None
    }
  }

  // Record a mapping from actor names to actor refs
  override def event_produced(event: Event) = {
    super.event_produced(event)
    handle_spawn_produced(event)
  }

  // Record that an event was consumed
  override def event_consumed(event: Event) = {
    handle_spawn_consumed(event)
  }

  // Record a message send event
  override def event_consumed(cell: ActorCell, envelope: Envelope) = {
    handle_event_consumed(cell, envelope)
  }

  override def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    enqueue_external_messages
    // Always prioritize external events.
    if (!pendingExternalEvents.isEmpty) {
      return Some(pendingExternalEvents.dequeue())
    }

    // Do we have some pending events
    if (pendingInternalEvents.isEmpty) {
      return None
    }
    return Some(pendingInternalEvents.removeRandomElement())
  }

  override def notify_quiescence () {
    handle_quiescence
  }

  // Shutdown the scheduler, this ensures that the instrumenter is returned to its
  // original pristine form, so one can change schedulers
  override def shutdown () = {
    handle_shutdown
  }

  // Notification that the system has been reset
  override def start_trace() : Unit = {
    handle_start_trace
  }

  override def after_receive(cell: ActorCell) : Unit = {
    handle_after_receive(cell)
  }

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }

  def reset_all_state () {
    pendingInternalEvents = new RandomizedHashSet[(ActorCell, Envelope)]
    pendingExternalEvents = new Queue[(ActorCell, Envelope)]
    actorNames = new HashSet[String]
    currentTime = 0
    // TODO(cs): also reset Instrumenter()'s state?
    reset_state
  }

  def test(events: Seq[ExternalEvent]) : Boolean = {
    Instrumenter().scheduler = this
    val execution = explore(events)
    reset_all_state
    // test passes if we were unable to find a failure.
    return execution == null
  }
}
