package akka.dispatch.verification

import akka.actor.ActorCell
import akka.actor.ActorRef
import akka.actor.Props;

import akka.dispatch.Envelope

// The interface for schedulers
trait Scheduler {
  
  
  def isSystemCommunication(sender: ActorRef, receiver: ActorRef): Boolean
  
  // Is this message a system message
  def isSystemMessage(src: String, dst: String): Boolean
  // Notification that the system has been reset
  def start_trace() : Unit
  // Get the next message to schedule
  def schedule_new_message() : Option[(ActorCell, Envelope)]  
  // Get next event to schedule (used while restarting the system)
  def next_event() : Event
  // Notify that there are no more events to run
  def notify_quiescence () : Unit
  // Called after receive is done being processed 
  def after_receive(cell: ActorCell) : Unit
  // Called before we start processing a newly received event
  def before_receive(cell: ActorCell) : Unit
  // Record that an event was produced 
  def event_produced(event: Event) : Unit
  def event_produced(cell: ActorCell, envelope: Envelope) : Unit
  // Record that an event was consumed
  def event_consumed(event: Event) : Unit  
  def event_consumed(cell: ActorCell, envelope: Envelope)
  // Tell the scheduler that it should eventually schedule the given message.
  // Used to feed messages from the external world into actor systems.
  def enqueue_message(receiver: String, msg: Any)
  // Shut down the actor system.
  def shutdown()

}

