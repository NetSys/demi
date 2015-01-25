package akka.dispatch.verification

import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

/**
 * Similar to PeekScheduler(), except that we start from mid-way in the
 * execution, and only Peek() for a small interval, and we schedule in FIFO
 * order rather than RR.
 *
 * Formal contract:
 *   Schedules a fixed number of unexpected messages in FIFO order
 *   to guess whether a particular message i is going to
 *   become enabled or not. If i does become enabled,
 *   we returns a prefix of messages that lead up
 *   to its being enabled. Otherwise, return null.
 */
class IntervalPeekScheduler(max_peek_messages: Int) extends ReplayScheduler {
  def this() = this(10)

  /*
   * peek() schedules a fixed number of unexpected messages in FIFO order
   * to guess whether a particular message event m is going to become enabled or not.
   *
   * If msg does become enabled, return a prefix of messages that lead up to its being enabled.
   *
   * Otherwise, return None.
   */
  def peek(prefix: EventTrace, lookingFor: MsgEvent, expected: Seq[Event]) : Option[Seq[MsgEvent]] = {
    // TODO(cs): implement me.
    return None
  }
}

/*
    def containedInExpected(msgEvent: MsgEvent) : Boolean = {
      val found = expected.find(e =>
        e match {
          case MsgEvent(sender, receiver, msg) =>
            msgEvent.sender == sender && msgEvent.receiver == receiver && msgEvent.msg == msg
          case _ => false
        }
      )
      return found != None
    }

    val prefix = new ListBuffer[MsgEvent]
    var triedSoFar = 0
    // STSScheduler.maxPeekMessagesToTry

    // TODO(cs): at this point the actor system has already been started, so
    // I'm not sure this is going to work.
    // val peeker = new PeekScheduler
    // Instrumenter().scheduler = peeker
    // val peekedEvents = peeker.peek(subseq)
    // assume(!peekedEvents.isEmpty)
    // assume(EventTypes.externalEventTypes.contains(peekedEvents(0).getClass))
    // peeker.shutdown
    // Instrumenter().scheduler = this
    // return peekedEvents
*/
