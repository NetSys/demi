package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.Envelope

import scala.collection.mutable.ListBuffer
import scala.collection.generic.Growable
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

// Internal api
case class UniqueMsgSend(m: MsgSend, id: Int) extends Event
case class UniqueMsgEvent(m: MsgEvent, id: Int) extends Event
case class UniqueTimerDelivery(t: TimerDelivery, id: Int) extends Event

case class EventTrace(val events: Queue[Event], var original_externals: Seq[ExternalEvent]) extends Growable[Event] with Iterable[Event] {
  def this() = this(new Queue[Event], null)
  def this(original_externals: Seq[ExternalEvent]) = this(new Queue[Event], original_externals)

  override def hashCode = this.events.hashCode
  override def equals(other: Any) : Boolean = other match {
    case that: EventTrace => this.events == that.events
    case _ => false
  }

  // Optional: if you have the original external events, that helps us with
  // filtering.
  def setOriginalExternalEvents(_original_externals: Seq[ExternalEvent]) = {
    original_externals = _original_externals
  }

  def copy() : EventTrace = {
    assume(original_externals != null)
    assume(!events.isEmpty)
    assume(!original_externals.isEmpty)
    return new EventTrace(new Queue[Event] ++ events,
                          new Queue[ExternalEvent] ++ original_externals)
  }

  // The difference between EventTrace.events and EventTrace.getEvents is that
  // we hide UniqueMsgSend/Events here
  // TODO(cs): this is a dangerous API; easy to mix this up with .events...
  def getEvents() : Seq[Event] = {
    return events.map(e =>
      e match {
        case UniqueMsgSend(m, id) => m
        case UniqueMsgEvent(m, id) => m
        case UniqueTimerDelivery(t: TimerDelivery, id) => t
        case i: Event => i
      }
    )
  }

  // This method should not be used to append MsgSends or MsgEvents
  override def +=(event: Event) : EventTrace.this.type = {
    events += event
    return this
  }

  def appendMsgSend(snd: String, rcv: String, msg: Any, id: Int) = {
    val m = UniqueMsgSend(MsgSend(snd, rcv, msg), id)
    this.+=(m)
  }

  def appendMsgEvent(pair: (ActorCell, Envelope), id: Int) = {
    val cell = pair._1
    val envelope = pair._2
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    val event = UniqueMsgEvent(MsgEvent(snd, rcv, msg), id)
    this.+=(event)
  }

  override def clear() : Unit = {
    events.clear
  }

  override def iterator() : Iterator[Event] = {
    return getEvents().iterator
  }

  // Ensure that all failure detector messages are pruned from the original trace,
  // since we are now in a divergent execution and the failure detector may
  // need to respond differently.
  def filterFailureDetectorMessages() : EventTrace = {
    def fromFD(snd: String, msg: Any) : Boolean = {
      if (snd != "deadLetters") {
        return false
      }

      return MessageTypes.fromFailureDetector(msg)
    }

    return new EventTrace(events.filterNot(e => e match {
        case UniqueMsgEvent(m, _) =>
          fromFD(m.sender, m.msg) || m.receiver == FailureDetector.fdName
        case UniqueMsgSend(m, _) =>
          fromFD(m.sender, m.msg) || m.receiver == FailureDetector.fdName
        case _ => false
      }
    ), original_externals)
  }

  def filterCheckpointMessages(): EventTrace = {
    return new EventTrace(events flatMap {
      case UniqueMsgEvent(MsgEvent(_, _, CheckpointRequest), _) => None
      case UniqueMsgEvent(MsgEvent(_, _, CheckpointReply(_)), _) => None
      case UniqueMsgSend(MsgSend(_, _, CheckpointRequest), _) => None
      case UniqueMsgSend(MsgSend(_, _, CheckpointReply(_)), _) => None
      case e => Some(e)
    }, original_externals)
  }

  // Filter all external events in original_trace that aren't in subseq.
  // As an optimization, also filter out some internal events that we know a priori
  // aren't going to occur in the subsequence execution.
  def subsequenceIntersection(subseq: Seq[ExternalEvent]) : EventTrace = {
    // Walk through all events in original_trace. As we walk through, check if
    // the current event corresponds to an external event at the head of subseq. If it does, we
    // include that in result, and pop off the head of subseq. Otherwise that external event has been
    // pruned and should not be included. All internal events are included in
    // result.
    var remaining = ListBuffer[ExternalEvent]() ++ subseq
    var result = new Queue[Event]()

    // N.B. we deal with external messages separately after this for loop,
    // since they're a bit trickier.
    // N.B. it'd be nicer to use a filter() here, but it isn't guarenteed to
    // iterate left to right.
    for (event <- events) {
      if (remaining.isEmpty) {
        if (!EventTypes.isExternal(event)) {
          result += event
        }
      } else {
        event match {
          case KillEvent(actor1) =>
            remaining(0) match {
              case Kill(actor2) => {
                if (actor1 == actor2) {
                  result += event
                  remaining = remaining.tail
                }
              }
              case _ => None
            }
          case PartitionEvent((a1,b1)) =>
            remaining(0) match {
              case Partition(a2,b2) => {
                if (a1 == a2 && b1 == b2) {
                  result += event
                  remaining = remaining.tail
                }
              }
              case _ => None
            }
          case UnPartitionEvent((a1,b1)) =>
            remaining(0) match {
              case UnPartition(a2,b2) => {
                if (a1 == a2 && b1 == b2) {
                  result += event
                  remaining = remaining.tail
                }
              }
              case _ => None
            }
          case SpawnEvent(_,_,name1,_) =>
            remaining(0) match {
              case Start(_, name2) => {
                if (name1 == name2) {
                  result += event
                  remaining = remaining.tail
                }
              }
              case _ => None
            }
          // We don't currently use ContextSwitches, so prune them to remove
          // clutter.
          case ChangeContext(_) => None
          // Always include all other internal events
          case i: Event => result += i
        } // close match
      } // close else
    } // close for
    return new EventTrace(filterSends(result, subseq), original_externals)
  }

  private[this] def filterSends(events: Queue[Event],
                                subseq: Seq[ExternalEvent]) : Queue[Event] = {
    // We assume that Send messages are sent in FIFO order, i.e. so that the
    // the zero-th MsgSend event from deadLetters corresponds to the zero-th Send event.

    // Our first task is to infer which Send events have been pruned.
    // We need original_externals to disambiguate which events have actually
    // been pruned, since it's possible that two MsgEvents or MsgSends at
    // different points of the trace are identical in terms of .equals.
    if (original_externals == null) {
      throw new IllegalArgumentException("Must invoke setOriginalExternalEvents")
    }

    // We assume that no one has copied the external events, i.e. the id's are
    // all the same.
    val original_sends = original_externals flatMap {
      case s: Send => Some(s)
      case _ => None
    }
    val subseq_sends_lst = subseq flatMap {
      case s: Send => Some(s)
      case _ => None
    }
    val subseq_sends = subseq_sends_lst.toSet

    // Gather all indexes of original_sends that are not in subseq_sends
    val missing_indices = new HashSet[Int] ++
        original_sends.zipWithIndex.foldRight(List[Int]())((tuple, lst) =>
      if (subseq_sends contains tuple._1) {
        lst
      } else {
        tuple._2 :: lst
      }
    )

    // Now filter out all external MsgSend/MsgEvents whose index is part of missing_indices
    var msg_send_idx = -1
    val pruned_msg_ids = new HashSet[Int]

    // N.B. it'd be nicer to use a filter() here, but it isn't guarenteed to
    // iterate left to right according to the spec.
    var remaining = new Queue[Event]()

    for (e <- events) {
      e match {
        case UniqueMsgSend(m, id) =>
          if (m.sender == "deadLetters") {
            msg_send_idx += 1
            if (!(missing_indices contains msg_send_idx)) {
              remaining += e
            } else {
              // We prune this event, and we need to later prune its corresponding
              // MsgEvent, if such an event exists.
              pruned_msg_ids += id
            }
          } else {
            remaining += e
          }
        case UniqueMsgEvent(m, id) =>
          if (!pruned_msg_ids.contains(id)) {
            remaining += e
          }
        case _ => remaining += e
      } // end match
    } // end for

    return filterKnownAbsentInternals(remaining, subseq)
  }

  // Remove internal events that we know a priori aren't going to occur for
  // this subsequence. In particular, if we pruned a "Start" event for a given
  // actor, we know that all messages destined to or coming from that actor
  // will not occur in the subsequence execution.
  private[this] def filterKnownAbsentInternals(events: Queue[Event],
                                               subseq: Seq[ExternalEvent]) : Queue[Event] = {
    var result = new Queue[Event]()

    // { actor name -> is the actor currently alive? }
    var actorToAlive = new HashMap[String, Boolean] {
      // If we haven't seen a Start event, that means it's non-existant.
      override def default(key:String) = false
    }
    actorToAlive("deadLetters") = true
    actorToAlive("Timer") = true

    // { (sender, receiver) -> can the actors send messages between eachother? }
    var actorsToPartitioned = new HashMap[(String, String), Boolean]() {
      // Somewhat confusing: if we haven't seen a Partition event, that means
      // it's reachable (so long as it's alive).
      // N.B. because we never partition the external world ("deadLetters")
      // from any actors, we always return false if either snd or rcv is
      // "deadLetters" or "Timer"
      override def default(key:(String, String)) = false
    }
    // IDs of message sends that we have pruned. Use case: if a message send
    // has been pruned, obviously its later message delivery won't ever occur.
    var prunedMessageSends = new HashSet[Int]

    def messageSendable(snd: String, rcv: String) : Boolean = {
      if (!actorToAlive(snd)) {
        return false
      }
      return !actorsToPartitioned((snd, rcv))
    }

    // TODO(cs): could probably be even more aggressive in pruning than we
    // currently are. For example, if we decide that a MsgEvent isn't going to
    // be delivered, then we could also prune its prior MsgSend, since it's a
    // no-op.
    def messageDeliverable(snd: String, rcv: String, id: Int) : Boolean = {
      if (!actorToAlive(rcv)) {
        return false
      }
      return !actorsToPartitioned((snd, rcv)) && !prunedMessageSends.contains(id)
    }

    for (event <- events) {
      event match {
        case UniqueMsgSend(m, id) =>
          if (messageSendable(m.sender, m.receiver)) {
            result += event
          } else {
            prunedMessageSends += id
          }
        case UniqueTimerDelivery(t, _) =>
          if (messageDeliverable(t.sender, t.receiver, -1)) {
            result += event
          }
        case UniqueMsgEvent(m, id) =>
          if (messageDeliverable(m.sender, m.receiver, id)) {
            result += event
          }
        case SpawnEvent(_, _, name, _) =>
          actorToAlive(name) = true
          result += event
        case KillEvent(name) =>
          actorToAlive(name) = false
          result += event
        case PartitionEvent((a,b)) =>
          actorsToPartitioned((a,b)) = false
          result += event
        case UnPartitionEvent((a,b)) =>
          actorsToPartitioned((a,b)) = true
          result += event
        case _ =>
          result += event
      }
    }
    return result
  }
}
