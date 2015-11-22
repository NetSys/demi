package akka.dispatch.verification

import akka.actor.{Cell, ActorRef, ActorSystem, Props}
import akka.dispatch.Envelope

import scala.collection.mutable.ListBuffer
import scala.collection.generic.Growable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable.Queue

// Internal api
case class UniqueMsgSend(m: MsgSend, id: Int) extends Event
case class UniqueMsgEvent(m: MsgEvent, id: Int) extends Event
case class UniqueTimerDelivery(t: TimerDelivery, id: Int) extends Event

case class EventTrace(val events: SynchronizedQueue[Event], var original_externals: Seq[ExternalEvent]) extends Growable[Event] with Iterable[Event] {
  def this() = this(new SynchronizedQueue[Event], null)
  def this(original_externals: Seq[ExternalEvent]) = this(new SynchronizedQueue[Event], original_externals)

  override def hashCode = this.events.hashCode
  override def equals(other: Any) : Boolean = other match {
    case that: EventTrace => this.events == that.events
    case _ => false
  }

  // Optional: if you have the original external events, that helps us with
  // filtering.
  def setOriginalExternalEvents(_original_externals: Seq[ExternalEvent]) = {
    println("Setting originalExternalEvents: " + _original_externals.size)
    original_externals = _original_externals
  }

  def copy() : EventTrace = {
    assume(original_externals != null)
    assume(!events.isEmpty)
    assume(!original_externals.isEmpty)
    // I can't figure out the type signature for SynchronizedQueue's ++ operator, so we
    // do it in two separate steps here
    val copy = new SynchronizedQueue[Event]
    copy ++= events
    return new EventTrace(copy,
                          new Queue[ExternalEvent] ++ original_externals)
  }

  // The difference between EventTrace.events and EventTrace.getEvents is that
  // we hide UniqueMsgSend/Events here
  // TODO(cs): this is a dangerous API; easy to mix this up with .events...
  def getEvents() : Seq[Event] = {
    return getEvents(events)
  }

  // Return any MsgSend events that were never delivered, i.e. they were
  // sitting in the buffer at the end of the execution.
  def getPendingMsgSends(): Set[MsgSend] = {
    val deliveredIds = events.flatMap {
      case UniqueMsgEvent(m, id) => Some(id)
      case _ => None
    }.toSet

    return events.flatMap {
      case UniqueMsgSend(m, id) if !(deliveredIds contains id) =>
        Some(m)
      case _ => None
    }.toSet
  }

  def length = events.length

  private[this] def getEvents(_events: Seq[Event]): Seq[Event] = {
    return _events.map(e =>
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

  def appendMsgEvent(pair: (Cell, Envelope), id: Int) = {
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

  // Take the result of ProvenanceTracker.pruneConcurrentEvents, and use that
  // to filter out any MsgEvents in our events that were pruned.
  // TODO(cs): have ProvenanceTracker act directly on us rather than on a
  // separate Queue[Unique].
  def intersection(uniqs: Queue[Unique], fingerprintFactory: FingerprintFactory) : EventTrace = {
    val msgEvents = new Queue[MsgEvent] ++ uniqs flatMap {
      case Unique(m: MsgEvent, id) =>
        // Filter out the root event
        if (id == 0) {
          None
        } else {
          Some(m)
        }
      case u => throw new IllegalArgumentException("Non MsgEvent:" + u)
    }

    // first pass: remove any UniqueMsgEvents that don't show up in msgEvents
    // track which UniqueMsgEvent ids were pruned
    val pruned = new HashSet[Int]
    var filtered = events flatMap {
      case u @ UniqueMsgEvent(m, id) =>
        msgEvents.headOption match {
          case Some(msgEvent) =>
            val fingerprinted = MsgEvent(m.sender, m.receiver,
              fingerprintFactory.fingerprint(m.msg))
            if (fingerprinted == msgEvent) {
              msgEvents.dequeue
              Some(u)
            } else {
              // u was filtered
              pruned += id
              None
            }
          case None =>
            // u was filtered
            pruned += id
            None
        }
      case t: TimerDelivery =>
        throw new UnsupportedOperationException("TimerDelivery not yet supported")
      case t: UniqueTimerDelivery =>
        throw new UnsupportedOperationException("UniqueTimerDelivery not yet supported")
      case m: MsgEvent =>
        throw new IllegalStateException("Should be UniqueMsgEvent")
      case e => Some(e)
    }

    // Should always be a strict subsequence
    assert(msgEvents.isEmpty)

    // second pass: remove any UniqueMsgSends that correspond to
    // UniqueMsgEvents that were pruned in the first pass
    // TODO(cs): not sure this is actually necessary.
    filtered = filtered flatMap {
      case u @ UniqueMsgSend(m, id) =>
        if (pruned contains id) {
          None
        } else {
          Some(u)
        }
      case e => Some(e)
    }

    val filteredQueue = new SynchronizedQueue[Event]
    filteredQueue ++= filtered

    return new EventTrace(filteredQueue, original_externals)
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

    val filtered = events.filterNot(e => e match {
        case UniqueMsgEvent(m, _) =>
          fromFD(m.sender, m.msg) || m.receiver == FailureDetector.fdName
        case UniqueMsgSend(m, _) =>
          fromFD(m.sender, m.msg) || m.receiver == FailureDetector.fdName
        case _ => false
      }
    )

    val filteredQueue = new SynchronizedQueue[Event]
    filteredQueue ++= filtered

    return new EventTrace(filteredQueue, original_externals)
  }

  def filterCheckpointMessages(): EventTrace = {
    val filtered = events flatMap {
      case UniqueMsgEvent(MsgEvent(_, _, CheckpointRequest), _) => None
      case UniqueMsgEvent(MsgEvent(_, _, CheckpointReply(_)), _) => None
      case UniqueMsgSend(MsgSend(_, _, CheckpointRequest), _) => None
      case UniqueMsgSend(MsgSend(_, _, CheckpointReply(_)), _) => None
      case e => Some(e)
    }

    val filteredQueue = new SynchronizedQueue[Event]
    filteredQueue ++= filtered

    return new EventTrace(filteredQueue, original_externals)
  }

  // Pre: externals corresponds exactly to our external MsgSend
  // events, i.e. subsequenceIntersection(externals) was used to create this
  // EventTrace.
  // Pre: no checkpoint messages in events
  def recomputeExternalMsgSends(externals: Seq[ExternalEvent]): Seq[Event] = {
    if (externals == null) {
      throw new IllegalStateException("original_externals must not be null")
    }
    val sends = externals flatMap {
      case s: Send => Some(s)
      case _ => None
    }
    if (sends.isEmpty) {
      return getEvents
    }
    val sendsQueue = Queue(sends: _*)

    // ---- Check an assertion: ----
    val sendsSet = new HashSet[UniqueMsgSend]
    events.foreach {
      case u @ UniqueMsgSend(MsgSend(snd, receiver, msg), id) =>
        if (sendsSet contains u) {
          throw new AssertionError("Duplicate UniqueMsgSend " + u + " " + events.mkString("\n"))
        }
        sendsSet += u
      case _ =>
    }
    // ----------------------

    return getEvents(events map {
      case u @ UniqueMsgSend(MsgSend(snd, receiver, msg), id) =>
        if (EventTypes.isExternal(u)) {
          if (sendsQueue.isEmpty) {
            // XXX
            // Problem seems to be some of the Send events that were actually
            // sent, don't appear in externals. Truncated somehow?
            println("events:---")
            events.foreach { case e => println(e) }
            println("---")
            println("externals:---")
            externals.foreach { case e => println(e) }
            println("---")
            throw new IllegalStateException("sendsQueue is empty, yet " + u)
          }
          val send = sendsQueue.dequeue
          val new_msg = send.messageCtor()
          UniqueMsgSend(MsgSend(snd, receiver, new_msg), id)
        } else {
          u
        }
      case m: MsgSend =>
        throw new IllegalArgumentException("Must be UniqueMsgSend")
      case e => e
    })
  }

  // Filter all external events in original_trace that aren't in subseq.
  // As an optimization, also filter out some internal events that we know a priori
  // aren't going to occur in the subsequence execution.
  def subsequenceIntersection(subseq: Seq[ExternalEvent],
                              filterKnownAbsents:Boolean=true) : EventTrace = {
    // Walk through all events in original_trace. As we walk through, check if
    // the current event corresponds to an external event at the head of subseq. If it does, we
    // include that in result, and pop off the head of subseq. Otherwise that external event has been
    // pruned and should not be included. All internal events are included in
    // result.
    // N.B. we deal with external messages separately after this for loop,
    // since they're a bit trickier.
    var remaining = ListBuffer[ExternalEvent]() ++ subseq.filter {
      case Send(_,_) => false
      case _ => true
    }
    var result = new Queue[Event]()

    // N.B. it'd be nicer to use a filter() here, but it isn't guarenteed to
    // iterate left to right.
    for (event <- events) {
      if (remaining.isEmpty) {
        if (EventTypes.isMessageType(event)) {
          result += event
        } else if (!EventTypes.isExternal(event) &&
                   !event.isInstanceOf[ChangeContext]) {
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
          case c @ CodeBlock(_) =>
            if (remaining(0) == c) {
              result += event
              remaining = remaining.tail
            }
          case h @ HardKill(_) =>
            if (remaining(0) == h) {
              result += event
              remaining = remaining.tail
            }
          // We don't currently use ContextSwitches, so prune them to remove
          // clutter.
          case ChangeContext(_) => None
          // Always include all other internal events
          case i: Event => result += i
        } // close match
      } // close else
    } // close for

    val filtered = filterSends(result, subseq, filterKnownAbsents=filterKnownAbsents)
    val filteredQueue = new SynchronizedQueue[Event]
    filteredQueue ++= filtered
    return new EventTrace(filteredQueue, original_externals)
  }

  private[this] def filterSends(events: Queue[Event],
                                subseq: Seq[ExternalEvent],
                                filterKnownAbsents:Boolean=true) : Queue[Event] = {
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

    // N.B. it'd be nicer to use a filter() here
    var remaining = new Queue[Event]()

    for (e <- events) {
      e match {
        case m @ UniqueMsgSend(msgEvent, id) =>
          if (EventTypes.isExternal(m)) {
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

    if (filterKnownAbsents) {
      return filterKnownAbsentInternals(remaining, subseq)
    }
    return remaining
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

// Encapsulates an EventTrace (which is immutable, hence why we need
// this class), along with:
//  (i) whether the EventTrace resulted in a violation
//  (ii) a mapping from Event to a list of console output messages that were
//  emitted as a result of executing that Event. For use with Synoptic.
class MetaEventTrace(val trace: EventTrace) {
  var causedViolation: Boolean = false
  // Invoked by schedulers to mark whether the violation was triggered.
  def setCausedViolation { causedViolation = true }

  // Invoked by schedulers to append log messages.
  val eventToLogOutput = new HashMap[Event,Queue[String]]
  def appendLogOutput(msg: String) {
    if (!(eventToLogOutput contains trace.events.last)) {
      eventToLogOutput(trace.events.last) = new Queue[String]
    }
    eventToLogOutput(trace.events.last) += msg
  }

  // Return an ordered sequence of log output messages emitted by the
  // application.
  def getOrderedLogOutput: Queue[String] = {
    val result = new Queue[String]
    trace.events.foreach {
      case e =>
        if (eventToLogOutput contains e) {
          result ++= eventToLogOutput(e)
        }
    }
    return result
  }
}
