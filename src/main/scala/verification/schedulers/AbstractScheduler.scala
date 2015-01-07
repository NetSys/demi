package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

// Basically just keeps track of actor names.
//
// Subclasses need to at least implement:
//   - schedule_new_message
//   - enqueue_message
//   - event_produced(cell: ActorCell, envelope: Envelope)
abstract class AbstractScheduler extends Scheduler {
  
  var instrumenter = Instrumenter()
  var currentTime = 0
  

  var actorNames = new HashSet[String]
  
  
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

  // Notification that the system has been reset
  def start_trace() : Unit = {
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
          actorNames += event.name
        }
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
  }

  def shutdown() {
    instrumenter.restart_system
  }

  // Get next event
  def next_event() : Event = {
    throw new Exception("NYI")
  }
}
