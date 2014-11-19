package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

// Just a very simple, non-null scheduler
class FairScheduler extends Scheduler {
  
  var instrumenter = Instrumenter()
  var currentTime = 0
  var index = 0
  
  // Current set of enabled events.
  val pendingEvents = new HashMap[String, Queue[(ActorCell, Envelope)]]  

  val actorNames = new HashSet[String]
  val actorQueue = new Queue[String]
  
  
  // Is this message a system message
  def isSystemCommunication(sender: ActorRef, receiver: ActorRef): Boolean = {
    if (sender == null && receiver == null) return true
    val senderPath = if (sender != null) sender.path.name else "deadLetters"
    val receiverPath = if (receiver != null) receiver.path.name else "deadLetters"
    return isSystemMessage(senderPath, receiverPath)
  }

  def isSystemMessage(src: String, dst: String): Boolean = {
    if ((actorNames contains src) || (actorNames contains dst))
      return false
    
    return true
  }
  
  def nextActor () : String = {
    val next = actorQueue(index)
    index = (index + 1) % actorQueue.size
    next
  }
  
  // Notification that the system has been reset
  def start_trace() : Unit = {
  }

  
  // Figure out what is the next message to schedule.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    val receiver = nextActor()
    // Do we have some pending events
    if (pendingEvents.isEmpty) {
      None
    } else { 
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
  
  // Get next event
  def next_event() : Event = {
    throw new Exception("NYI")
  }

  // Record that an event was consumed
  def event_consumed(event: Event) = {
  }
  
  def event_consumed(cell: ActorCell, envelope: Envelope) = {
  }
  
  // Record that an event was produced 
  def event_produced(event: Event) = {
    event match {
      case event : SpawnEvent => 
        if (!(actorNames contains event.name)) {
          println("Sched knows about actor " + event.name)
          actorQueue += event.name
          actorNames += event.name
        }
    }
  }
  
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
  
  // Called before we start processing a newly received event
  def before_receive(cell: ActorCell) {
    currentTime += 1
  }
  
  // Called after receive is done being processed 
  def after_receive(cell: ActorCell) {
  }
  
  def notify_quiescence () {
    println("No more messages to process " + pendingEvents)
  }
}
