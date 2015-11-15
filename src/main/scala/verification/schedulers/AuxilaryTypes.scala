package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Semaphore,
       scala.collection.mutable.HashMap,
       scala.collection.mutable.HashSet

// ----------- Internal event types -------------
abstract trait Event

// Metadata events, not actually events.
// MsgEvents appearing between `BeginUnignorableEvents' and `EndUnigorableEvents'
// will never be skipped over during replay.
final case object BeginUnignorableEvents extends Event
final case object EndUnignorableEvents extends Event
// An external thread has just started an `atomic block`, where it will now
// send some number of messages. Upon replay, wait until the end of the
// atomic block before deciding whether those messages are or are not going
// to show up.
final case class BeginExternalAtomicBlock(taskId: Long) extends Event
final case class EndExternalAtomicBlock(taskId: Long) extends Event

// Internal events.
// MsgSend is the initial send, not the delivery
// N.B., if an event trace was serialized, it's possible that msg is of type
// MessageFingerprint rather than a whole message!

// Message delivery -- (not the initial send)
// N.B., if an event trace was serialized, it's possible that msg is of type
// MessageFingerprint rather than a whole message!
case class MsgEvent(
    sender: String, receiver: String, msg: Any) extends Event
case class SpawnEvent(
    parent: String, props: Props, name: String, actor: ActorRef) extends Event

// (Used by DPOR)
// TODO(cs): consolidate with redundant types below.
case class NetworkPartition(
    first: Set[String],
    second: Set[String]) extends
  UniqueExternalEvent with ExternalEvent with Event
// (Used by DPOR)
case class NetworkUnpartition(
    first: Set[String],
    second: Set[String]) extends
  UniqueExternalEvent with ExternalEvent with Event

// (More general than DPOR)
final case class MsgSend (sender: String,
                receiver: String, msg: Any) extends Event
final case class KillEvent (actor: String) extends Event 
final case class PartitionEvent (endpoints: (String, String)) extends Event
final case class UnPartitionEvent (endpoints: (String, String)) extends Event
// Marks when WaitQuiescence was first processed.
final case object BeginWaitQuiescence extends Event
// Marks when Quiescence was actually reached.
final case object Quiescence extends Event
final case class ChangeContext (actor: String) extends Event

// Recording/Replaying Akka.FSM.Timer's (which aren't serializable! hence this madness)
// N.B. these aren't explicitly recorded. We use them only when we want to serialize event
// traces.
final case class TimerFingerprint(name: String,
  msgFingerprint: MessageFingerprint, repeat: Boolean, generation: Int) extends MessageFingerprint
// Corresponds to MsgEvent.
final case class TimerDelivery(sender: String, receiver: String, fingerprint: TimerFingerprint) extends Event

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

// Shared instance
object ExploredTacker {
  var obj:ExploredTacker = null
  def apply() = {
    if (obj == null) {
      obj = new ExploredTacker
    }
    obj
  }
}
