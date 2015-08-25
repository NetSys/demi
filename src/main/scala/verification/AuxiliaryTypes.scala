package akka.dispatch.verification

import akka.actor.{ ActorCell, ActorRef, Props }
import akka.dispatch.{ Envelope }

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Semaphore,
       scala.collection.mutable.HashMap,
       scala.collection.mutable.HashSet


object IDGenerator {
  var uniqueId = new AtomicInteger // DPOR root event is assumed to be ID 0, incrementAndGet ensures starting at 1

  def get() : Integer = {
    val id = uniqueId.incrementAndGet()
    if (id == Int.MaxValue) {
      throw new RuntimeException("Deal with overflow..")
    }
    return id
  }
}

       
case class Unique(
  val event : Event,
  var id : Int = IDGenerator.get()
) extends ExternalEvent {
  def label: String = "e"+id
}

case class Uniq[E](
  val element : E,
  var id : Int = IDGenerator.get()
)

// Message delivery -- (not the initial send)
// N.B., if an event trace was serialized, it's possible that msg is of type
// MessageFingerprint rather than a whole message!
case class MsgEvent(
    sender: String, receiver: String, msg: Any) extends Event

case class SpawnEvent(
    parent: String, props: Props, name: String, actor: ActorRef) extends Event

case class NetworkPartition(
    first: Set[String],
    second: Set[String]) extends
  UniqueExternalEvent with ExternalEvent with Event

case class NetworkUnpartition(
    first: Set[String],
    second: Set[String]) extends
  UniqueExternalEvent with ExternalEvent with Event

case object RootEvent extends Event

case class WildCardMatch(
  // Given:
  //   - a list of pending messages, sorted from least recently to most recently sent
  //   - a "backtrack setter" function: given an index of the pending
  //     messages, sets a backtrack point for that pending message, to be
  //     replayed in the future.
  // return the index of the chosen one, or None
  msgSelector: (Seq[Any], (Int) => Unit) => Option[Int],
  name:String=""
)

// Base class for failure detector messages
abstract class FDMessage

// Notification telling a node that it can query a failure detector by sending messages to fdNode.
case class FailureDetectorOnline(fdNode: String) extends FDMessage

// A node is unreachable, either due to node failure or partition.
case class NodeUnreachable(actor: String) extends FDMessage with Event

case class NodesUnreachable(actors: Set[String]) extends FDMessage with Event


// A new node is now reachable, either because a partition healed or an actor spawned.
case class NodeReachable(actor: String) extends FDMessage with Event
//
// A new node is now reachable, either because a partition healed or an actor spawned.
case class NodesReachable(actors: Set[String]) extends FDMessage with Event

// Query the failure detector for currently reachable actors.
case object QueryReachableGroup extends FDMessage

// Response to failure detector queries.
case class ReachableGroup(actors: Set[String]) extends FDMessage

object MessageTypes {
  // Messages that the failure detector sends to actors.
  // Assumes that actors don't relay fd messages to eachother.
  def fromFailureDetector(m: Any) : Boolean = {
    m match {
      case _: FailureDetectorOnline | _: NodeUnreachable | _: NodeReachable |
           _: ReachableGroup => return true
      case _ => return false
    }
  }

  def fromCheckpointCollector(m: Any) : Boolean = {
    m match {
      case CheckpointRequest => return true
      case _ => return false
    }
  }

  def sanityCheckTrace(trace: Seq[ExternalEvent]) {
    trace foreach {
      case Send(_, msgCtor) =>
        val msg = msgCtor()
        if (MessageTypes.fromFailureDetector(msg) ||
            MessageTypes.fromCheckpointCollector(msg)) {
          throw new IllegalArgumentException(
            "trace contains system message: " + msg)
        }
      case _ => None
    }
  }
}

object ActorTypes {
  def systemActor(name: String) : Boolean = {
    return name == FailureDetector.fdName || name == CheckpointSink.name
  }
}

/**
 * TellEnqueue is a semaphore that ensures a linearizable execution, and protects
 * schedulers' data structures during akka's concurrent processing of `tell`
 * (the `!` operator).
 *
 * Instrumenter()'s control flow is as follows:
 *  - Invoke scheduler.schedule_new_message to find a new message to deliver
 *  - Call `dispatcher.dispatch` to deliver the message. Note that
 *    `dispatcher.dispatch` hands off work to a separate thread and returns
 *    immediately.
 *  - The actor `receive()`ing the message now becomes active
 *  - Every time that actor invokes `tell` to send a message to a known actor,
 *    a ticket is taken from TellEnqueue via TellEnqueue.tell()
 *  - Concurrently, akka will process the `tell`s by enqueuing the message in
 *    the receiver's mailbox.
 *  - Every time akka finishes enqueueing a message to the recevier's mailbox,
 *    we first call scheduler.event_produced, and then replaces a ticket to
 *    TellEnqueue via TellEnqueue.enqueue()
 *  - When the actor returns from `receive`, we wait for all tickets to be
 *    returned (via TellEnqueue.await()) before scheduling the next message.
 *
 * The `known actor` part is crucial. If the receiver is not an actor (e.g.
 * the main thread) or we do not interpose on the receiving actor, we will not
 * be able to return the ticket via TellEnqueue.enqueue(), and the system will
 * block forever on TellEnqueue.await().
 */
trait TellEnqueue {
  def tell()
  def enqueue()
  def reset()
  def await ()
}

class TellEnqueueBusyWait extends TellEnqueue {
  
  var enqueue_count = new AtomicInteger
  var tell_count = new AtomicInteger
  
  def tell() {
    tell_count.incrementAndGet()
  }
  
  def enqueue() {
    enqueue_count.incrementAndGet()
  }
  
  def reset() {
    tell_count.set(0)
    enqueue_count.set(0)
  }

  def await () {
    while (tell_count.get != enqueue_count.get) {}
  }
  
}
    

class TellEnqueueSemaphore extends Semaphore(1) with TellEnqueue {
  
  var enqueue_count = new AtomicInteger
  var tell_count = new AtomicInteger
  
  def tell() {
    tell_count.incrementAndGet()
    reducePermits(1)
    require(availablePermits() <= 0)
  }

  def enqueue() {
    enqueue_count.incrementAndGet()
    require(availablePermits() <= 0)
    release()
  }
  
  def reset() {
    tell_count.set(0)
    enqueue_count.set(0)
    // Set available permits to 0
    drainPermits() 
    // Add a permit
    release()
  }
  
  def await() {
    acquire
    release
  }
}

class ExploredTacker {
  var exploredStack = new HashMap[Int, HashSet[(Unique, Unique)] ]
  
  def setExplored(index: Int, pair: (Unique, Unique)) =
  exploredStack.get(index) match {
    case Some(set) => set += pair
    case None =>
      val newElem = new HashSet[(Unique, Unique)] + pair
      exploredStack(index) = newElem
  }
  
  def isExplored(pair: (Unique, Unique)): Boolean = {

    for ((index, set) <- exploredStack) set.contains(pair) match {
      case true => return true
      case false =>
    }

    return false
  }
  
  def trimExplored(index: Int) = {
    exploredStack = exploredStack.filter { other => other._1 <= index }
  }

  
  def printExplored() = {
    for ((index, set) <- exploredStack.toList.sortBy(t => (t._1))) {
      println(index + ": " + set.size)
      //val content = set.map(x => (x._1.id, x._2.id))
      //println(index + ": " + set.size + ": " +  content))
    }
  }

  def clear() = {
    exploredStack.clear()
  }
}

object ExploredTacker {
  var obj:ExploredTacker = null
  def apply() = {
    if (obj == null) {
      obj = new ExploredTacker
    }
    obj
  }
}
