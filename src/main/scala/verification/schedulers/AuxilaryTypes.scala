package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope}

// External events used to specify a trace
abstract trait ExternalEvent

final case class Start (prop: Props, name: String) extends ExternalEvent
final case class Kill (name: String) extends ExternalEvent {}
final case class Send (name: String, message: Any) extends ExternalEvent
final case object WaitQuiescence extends ExternalEvent
// Bidirectional partitions.
final case class Partition (a: String, b: String) extends ExternalEvent
final case class UnPartition (a: String, b: String) extends ExternalEvent

// Internal events in addition to those defined in ../AuxilaryTypes
// MsgSend is the initial send, not the delivery
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

object EventTypes {
  // Internal events that correspond to ExternalEvents.
  def isExternal(e: Event) : Boolean = {
    return e match {
      case _: KillEvent | _: SpawnEvent | _: PartitionEvent | _: UnPartitionEvent =>
        return true
      case MsgEvent(snd, _, _) =>
        return snd == "deadLetters"
      case MsgSend(snd, _, _) =>
        return snd == "deadLetters"
      case _ => return false
    }
  }
}
