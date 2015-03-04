package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.{Envelope}

// External events used to specify a trace
abstract trait ExternalEvent
abstract trait Event

final case class Start (propCtor: () => Props, name: String) extends Event with ExternalEvent
final case class Kill (name: String) extends Event with ExternalEvent {}
// Allow the client to late-bind the construction of the message. Invoke the
// function at the point that the Send is about to be injected.
final case class Send (name: String, messageCtor: () => Any) extends Event with ExternalEvent
final case object WaitQuiescence extends Event with ExternalEvent
// Wait for numTimers currently queued timers to be scheduled. numTimers can be set to
// -1 to wait for all currently queued timers.
final case class WaitTimers(numTimers: Integer) extends Event with ExternalEvent
// Bidirectional partitions.
final case class Partition (a: String, b: String) extends Event with ExternalEvent
final case class UnPartition (a: String, b: String) extends Event with ExternalEvent
// Continue scheduling numSteps internal events. Whenver we arrive at
// quiescence, wait for the next timer, then wait for quiescence, etc. until
// numSteps messages have been sent.
final case class Continue(numSteps: Integer) extends Event with ExternalEvent

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
final case class TimerFingerprint(name: String, sender: String, receiver: String,
  msgFingerprint: MessageFingerprint, repeat: Boolean, generation: Int)
final case class TimerSend(fingerprint: TimerFingerprint) extends Event
final case class TimerDelivery(fingerprint: TimerFingerprint) extends Event


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
