package akka.dispatch.verification

import akka.actor.ActorCell
import akka.actor.ActorRef
import akka.actor.Props;

import akka.dispatch.Envelope

// The interface for schedulers
trait Scheduler {

  def isSystemCommunication(sender: ActorRef, receiver: ActorRef, msg: Any): Boolean = 
    isSystemCommunication(sender, receiver)
  
  def isSystemCommunication(sender: ActorRef, receiver: ActorRef): Boolean
  
  // Is this message a system message
  def isSystemMessage(src: String, dst: String): Boolean
  
  def isSystemMessage(src: String, dst: String, msg: Any): Boolean =
    isSystemMessage(src, dst)
    
  // Notification that the system has been reset
  def start_trace() : Unit
  // Get the next message to schedule
  def schedule_new_message() : Option[(ActorCell, Envelope)]  
  // Get next event to schedule (used while restarting the system)
  def next_event() : Event
  // Notify that there are no more events to run
  def notify_quiescence () : Unit
  
  // Called before we start processing a newly received event
  def before_receive(cell: ActorCell) : Unit
  // Called after receive is done being processed 
  def after_receive(cell: ActorCell) : Unit
  def continue_scheduling(cell: ActorCell, msg: Any) : Boolean =
    return true
  
  def after_receive(cell: ActorCell, msg: Any) : Unit =
    after_receive(cell)
    
  def before_receive(cell: ActorCell, msg: Any) : Unit =
    before_receive(cell)
    
  // Record that an event was produced 
  def event_produced(event: Event) : Unit
  def event_produced(cell: ActorCell, envelope: Envelope) : Unit
  // Record that an event was consumed
  def event_consumed(event: Event) : Unit  
  def event_consumed(cell: ActorCell, envelope: Envelope)

}

