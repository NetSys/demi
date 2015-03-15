package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope}

// External events used to specify a trace
abstract trait ExternalEvent

trait UniqueExternalEvent {
  val _id : Int = IDGenerator.get()

  def toStringWithId: String = "e"+_id+":"+toString()

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

final case class Start (propCtor: () => Props, name: String) extends
    ExternalEvent with Event with UniqueExternalEvent
final case class Kill (name: String) extends
    ExternalEvent with Event with UniqueExternalEvent {}
// Allow the client to late-bind the construction of the message. Invoke the
// function at the point that the Send is about to be injected.
final case class Send (name: String, messageCtor: () => Any) extends
    ExternalEvent with Event with UniqueExternalEvent
final case class WaitQuiescence() extends
    ExternalEvent with Event with UniqueExternalEvent
// Bidirectional partitions.
final case class Partition (a: String, b: String) extends
    ExternalEvent with Event with UniqueExternalEvent
final case class UnPartition (a: String, b: String) extends
    ExternalEvent with Event with UniqueExternalEvent

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
final case class TimerDelivery(sender: String, receiver: String, fingerprint: TimerFingerprint) extends Event


object EventTypes {
  // Internal events that correspond to ExternalEvents.
  def isExternal(e: Event) : Boolean = {
    return e match {
      case _: KillEvent | _: SpawnEvent | _: PartitionEvent | _: UnPartitionEvent =>
        return true
      case MsgEvent(snd, _, _) =>
        return snd == "deadLetters" // TODO(cs): Timers break this
      case MsgSend(snd, _, _) =>
        return snd == "deadLetters"
      case UniqueMsgEvent(MsgEvent(snd, _, _), _) =>
        return snd == "deadLetters"
      case UniqueMsgSend(MsgSend(snd, _, _), _) =>
        return snd == "deadLetters"
      case _ => return false
    }
  }
}
