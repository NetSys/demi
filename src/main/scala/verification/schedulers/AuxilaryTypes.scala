package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope}

// External events used to specify a trace
abstract class ExternalEvent {
  // Ensure that ExternalEvents are unique
  val id = IDGenerator.get()

  override def equals(other: Any) : Boolean = {
    other match {
      case o: ExternalEvent => return id == o.id
      case _ => return false
    }
  }

  override def hashCode : Int = {
    return id
  }
}

final case class Start (prop: Props, name: String) extends ExternalEvent
final case class Kill (name: String) extends ExternalEvent {}
final case class Send (name: String, message: Any) extends ExternalEvent
final case object WaitQuiescence extends ExternalEvent
final case class Partition (a: String, b: String) extends ExternalEvent
final case class UnPartition (a: String, b: String) extends ExternalEvent

// Internal events in addition to those defined in ../AuxilaryTypes
final case class MsgSend (sender: String, 
                receiver: String, msg: Any) extends Event
final case class KillEvent (actor: String) extends Event 
final case class PartitionEvent (endpoints: (String, String)) extends Event
final case class UnPartitionEvent (endpoints: (String, String)) extends Event
final case object Quiescence extends Event 
final case class ChangeContext (actor: String) extends Event
