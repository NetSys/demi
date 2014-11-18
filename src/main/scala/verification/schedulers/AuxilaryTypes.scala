package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}

// External events used to specify a trace
abstract class ExternalEvent
final case class Start (prop: Props, name: String) extends ExternalEvent
final case class Kill (name: String) extends ExternalEvent {}
final case class Send (name: String, message: Any) extends ExternalEvent
final case object WaitQuiescence extends ExternalEvent
final case class Partition (a: String, b: String) extends ExternalEvent
final case class UnPartition (a: String, b: String) extends ExternalEvent

// Internal events in addition to those defined in ../AuxilaryTypes
final case class KillEvent (actor: String) extends Event 
final case class PartitionEvent (endpoints: (String, String)) extends Event
final case class UnPartitionEvent (endpoints: (String, String)) extends Event
final case object Quiescence extends Event 
