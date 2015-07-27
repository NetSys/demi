package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope}

// External events used to specify a trace
abstract trait ExternalEvent {
  def label: String
}

trait UniqueExternalEvent {
  var _id : Int = IDGenerator.get()

  def label: String = "e"+_id
  def toStringWithId: String = label+":"+toString()

  override def equals(other: Any): Boolean = {
    if (other.isInstanceOf[UniqueExternalEvent]) {
      return _id == other.asInstanceOf[UniqueExternalEvent]._id
    } else {
      return false
    }
  }

  override def hashCode: Int = {
    return _id
  }
}
abstract trait Event

/**
 * ExternalMessageConstructors are instance variables of Send() events.
 * They serve two purposes:
 *  - They allow the client to late-bind the construction of their message.
 *    apply() is invoked after the ActorSystem and all actors have been
 *    created.
 *  - Optionally: they provide an interface for `shrinking` the contents of
 *    the messages. This is achieved through `getComponents` and
 *    `maskComponents`.
 */
// TODO(cs): should probably force this to be serializable
trait ExternalMessageConstructor {
  // Construct the message
  def apply() : Any
  // Optional, for `shrinking`:
  // Get the components that make up the content of the message we construct
  // in apply(). For now, only relevant to cluster membership messages.
  def getComponents() : Seq[ActorRef] = List.empty
  // Given a sequence of indices (pointing to elements in `getComponents()`),
  // create a new ExternalMessageConstructor that does not include those
  // components upon apply().
  // Default: no-op
  def maskComponents(indices: Set[Int]): ExternalMessageConstructor = this
}

case class BasicMessageConstructor(msg: Any) extends ExternalMessageConstructor {
  def apply(): Any = msg
}

final case class Start (propCtor: () => Props, name: String) extends
    ExternalEvent with Event with UniqueExternalEvent
// Really: isolate the actor.
final case class Kill (name: String) extends
    ExternalEvent with Event with UniqueExternalEvent
// Actually kill the actor rather than just isolating it.
// TODO(cs): support killing of actors that aren't direct children of /user/
final case class HardKill (name: String) extends
    ExternalEvent with Event with UniqueExternalEvent
final case class Send (name: String, messageCtor: ExternalMessageConstructor) extends
    ExternalEvent with Event with UniqueExternalEvent
final case class WaitQuiescence() extends
    ExternalEvent with Event with UniqueExternalEvent
// Stronger than WaitQuiescence: schedule indefinitely until cond returns true.
// if quiescence has been reached but cond does
// not return true, wait indefinitely until scheduler.enqueue_message is
// invoked, schedule it, and again wait for quiescence. Repeat until cond
// returns true. (Useful for systems that use external threads to send
// messages indefinitely.
final case class WaitCondition(cond: () => Boolean) extends
    ExternalEvent with Event with UniqueExternalEvent
// Bidirectional partitions.
final case class Partition (a: String, b: String) extends
    ExternalEvent with Event with UniqueExternalEvent
final case class UnPartition (a: String, b: String) extends
    ExternalEvent with Event with UniqueExternalEvent
// Executed synchronously, i.e. by the scheduler itself. The code block must
// terminate (quickly)!
final case class CodeBlock (block: () => Any) extends
    ExternalEvent with Event with UniqueExternalEvent

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


// Internal events in addition to those defined in ../AuxilaryTypes
// MsgSend is the initial send, not the delivery
// N.B., if an event trace was serialized, it's possible that msg is of type
// MessageFingerprint rather than a whole message!
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


object EventTypes {
  // Should return true if the given message is an external message
  var externalMessageFilter: (Any) => Boolean = (_) => false
  // Should be set by applications during initialization.
  def setExternalMessageFilter(filter: (Any) => Boolean) {
    externalMessageFilter = filter
  }

  def isMessageType(e: Event) : Boolean = {
    e match {
      case MsgEvent(_, _, m) =>
        return true
      case MsgSend(_, _, m) =>
        return true
      case UniqueMsgEvent(MsgEvent(_, _, m), _) =>
        return true
      case UniqueMsgSend(MsgSend(_, _, m), _) =>
        return true
      case _ =>
        return false
    }
  }

  // Internal events that correspond to ExternalEvents.
  def isExternal(e: Event) : Boolean = {
    if (e.isInstanceOf[ExternalEvent]) {
      return true
    }
    return e match {
      case _: KillEvent | _: SpawnEvent | _: PartitionEvent | _: UnPartitionEvent =>
        return true
      case MsgEvent(_, _, m) =>
        return externalMessageFilter(m)
      case MsgSend(_, _, m) =>
        return externalMessageFilter(m)
      case UniqueMsgEvent(MsgEvent(_, _, m), _) =>
        return externalMessageFilter(m)
      case UniqueMsgSend(MsgSend(_, _, m), _) =>
        return externalMessageFilter(m)
      case _ => return false
    }
  }
}
