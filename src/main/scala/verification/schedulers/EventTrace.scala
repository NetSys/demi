package akka.dispatch.verification

import scala.collection.mutable.ListBuffer
import scala.collection.generic.Growable
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet

class EventTrace(events: Queue[Event], var original_externals: Seq[ExternalEvent]) extends Growable[Event] with Iterable[Event] {
  def this() = this(new Queue[Event], null)
  def this(events: Queue[Event]) = this(events, null)
  def this(original_externals: Seq[ExternalEvent]) = this(new Queue[Event], original_externals)

  // Optional: if you have the original external events, that helps us with
  // filtering.
  def setOriginalExternalEvents(_original_externals: Seq[ExternalEvent]) = {
    original_externals = _original_externals
  }

  def getEvents() : Seq[Event] = {
    return events
  }

  override def +=(event: Event) : EventTrace.this.type = {
    events += event
    return this
  }

  override def clear() : Unit = {
    events.clear
  }

  override def iterator() : Iterator[Event] = {
    return events.iterator
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
        case MsgEvent(snd, rcv, msg) =>
          fromFD(snd, msg) || rcv == FailureDetector.fdName
        case MsgSend(snd, rcv, msg) =>
          fromFD(snd, msg) || rcv == FailureDetector.fdName
        case _ => false
      }
    ), original_externals)
  }

  // Filter all external events in original_trace that aren't in subseq.
  def subsequenceIntersection(subseq: Seq[ExternalEvent]) : EventTrace = {
    // Walk through all events in original_trace. As we walk through, check if
    // the current event corresponds to an external event at the head of subseq. If it does, we
    // include that in result, and pop off the head of subseq. Otherwise that external event has been
    // pruned and should not be included. All internal events are included in
    // result.
    var remaining = ListBuffer[ExternalEvent]() ++ subseq
    var result = ArrayBuffer[Event]()

    // N.B. we deal with external events separately after this for loop,
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

  private[this] def filterSends(events: ArrayBuffer[Event],
                                subseq: Seq[ExternalEvent]) : Queue[Event] = {
    // We assume that external messages are dispatched in FIFO order, so that
    // the zero-th MsgSend from deadLetters and the zero-th MsgEvent from
    // deadLetters both correspond to the zero-th Send event.

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
    val subseq_sends = new HashSet[Send] ++ subseq flatMap {
      case s: Send => Some(s)
      case _ => None
    }

    // Gather all indexes of original_sends that are not in subseq_sends
    val missing_indices = new HashSet[Int] ++
        original_sends.zipWithIndex.foldRight(List[Int]())((tuple, lst) =>
      if (subseq_sends contains tuple._1) {
        lst
      } else {
        tuple._2 :: lst
      }
    )

    // Now filter out all external MsgSend/MsgEvents whose indix is part of missing_indices
    var msg_send_idx = -1
    var msg_event_idx = -1

    // N.B. it'd be nicer to use a filter() here, but it isn't guarenteed to
    // iterate left to right according to the spec.
    var remaining = new Queue[Event]()

    for (e <- events) {
      e match {
        case MsgSend(snd, _, _) =>
          if (snd == "deadLetters") {
            msg_send_idx += 1
            if (!(missing_indices contains msg_send_idx)) {
              remaining += e
            }
          } else {
            remaining += e
          }
        case MsgEvent(snd, _, _) =>
          if (snd == "deadLetters") {
            msg_event_idx += 1
            if (!(missing_indices contains msg_event_idx)) {
              remaining += e
            }
          } else {
            remaining += e
          }
        case _ => remaining += e
      } // end match
    } // end for

    return remaining
  }
}
