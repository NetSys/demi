package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

/**
 * Schedules events in a round-robin fashion.
 */
class FairScheduler extends AbstractScheduler {
  var actorQueue = new Queue[String]
  var index = 0

  // Current set of enabled events.
  val pendingEvents = new HashMap[String, Queue[Uniq[(ActorCell, Envelope)]]]  

  def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[Uniq[(ActorCell, Envelope)]])
    
    val uniq = Uniq[(ActorCell, Envelope)]((cell, envelope))
    pendingEvents(rcv) = msgs += uniq
    // Start dispatching events
    if (!instrumenter.started.get) {
      instrumenter.start_dispatch()
    }
  }

  def nextActor () : String = {
    if (actorQueue.size == 0) {
      actorQueue = actorQueue ++ actorNames.toList
    }
    val next = actorQueue(index)
    index = (index + 1) % actorQueue.size
    next
  }
  
  def find_message_to_schedule() : Option[Uniq[(ActorCell, Envelope)]] = {
    val receiver = nextActor()
    // Do we have some pending events
    if (pendingEvents.isEmpty) {
      None
    } else { 
      pendingEvents.get(receiver) match {
        case Some(queue) =>
          if (queue.isEmpty == true) {
            pendingEvents.remove(receiver) match {
              case Some(key) => find_message_to_schedule()
              case None => throw new Exception("Internal error") // Really this is saying pendingEvents does not have 
                                                                 // receiver as a key
            }
          } else {
            val uniq = queue.dequeue()
            Some(uniq)
          }
        case None =>
          find_message_to_schedule()
      }
    }
  }
  
  // Figure out what is the next message to schedule.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    find_message_to_schedule match {
      case Some(uniq) =>
        return Some(uniq.element)
      case None =>
        return None
    }
  }
  
  def enqueue_message(receiver: String, msg: Any) {
    throw new Exception("NYI")
  }

  override def notify_quiescence () {
    println("No more messages to process " + pendingEvents)
  }
}
