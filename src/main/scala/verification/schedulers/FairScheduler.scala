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
  val pendingEvents = new HashMap[String, Queue[(ActorCell, Envelope)]]  

  def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])
    
    pendingEvents(rcv) = msgs += ((cell, envelope))
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
  
  
  // Figure out what is the next message to schedule.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    val receiver = nextActor()
    // Do we have some pending events
    if (pendingEvents.isEmpty) {
      None
    } else { 
      // TODO(cs): This code is incomprehensible to me...
      pendingEvents.get(receiver) match {
        case Some(queue) =>
          if (queue.isEmpty == true) {
            pendingEvents.remove(receiver) match {
              case Some(key) => schedule_new_message()
              case None => throw new Exception("Internal error") // Really this is saying pendingEvents does not have 
                                                                 // receiver as a key
            }
          } else {
            Some(queue.dequeue())
          }
        case None =>
          schedule_new_message()
      }
    }
  }
  
  def enqueue_message(receiver: String, msg: Any) {
    throw new Exception("NYI")
  }

  override def notify_quiescence () {
    println("No more messages to process " + pendingEvents)
  }
}
