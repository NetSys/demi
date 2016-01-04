package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.{ Envelope }

// ---------- External Events -----------

// External events used to specify a trace
abstract trait ExternalEvent {
  def label: String
}

// Attaches unique IDs to external events.
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
abstract class ExternalMessageConstructor extends Serializable {
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

// External Event types.
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

// ---------- Failure Detector messages -----------

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


// --------------- Utility functions ----------------

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

object EventTypes {
  // Should return true if the given message is an external message
  var externalMessageFilterHasBeenSet = false
  var externalMessageFilter: (Any) => Boolean = (_) => false
  // Should be set by applications during initialization.
  def setExternalMessageFilter(filter: (Any) => Boolean) {
    externalMessageFilterHasBeenSet = true
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
