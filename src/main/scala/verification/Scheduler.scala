package akka.dispatch.verification

import akka.actor.Cell
import akka.actor.ActorRef
import akka.actor.Props;
import akka.actor.AutoReceivedMessage;
import akka.actor.ActorIdentity;
import akka.actor.ReceiveTimeout;

import akka.dispatch.Envelope

// The interface for schedulers
trait Scheduler {
  
  def isSystemCommunication(sender: ActorRef, receiver: ActorRef): Boolean
  def isSystemCommunication(sender: ActorRef, receiver: ActorRef, msg: Any): Boolean = {
    if (msg.isInstanceOf[AutoReceivedMessage] ||
        msg.isInstanceOf[ActorIdentity] ||
        msg.isInstanceOf[ReceiveTimeout]) {
      return true
    }
    return isSystemCommunication(sender, receiver)
  }
    
    
  // Is this message a system message
  def isSystemMessage(src: String, dst: String): Boolean
  def isSystemMessage(src: String, dst: String, msg: Any): Boolean = {
    if (msg.isInstanceOf[AutoReceivedMessage] ||
        msg.isInstanceOf[ActorIdentity] ||
        msg.isInstanceOf[ReceiveTimeout]) {
      return true
    }
    return isSystemMessage(src, dst)
  }
    
  // Notification that the system has been reset
  def start_trace() : Unit
  // Get the next message to schedule. Make sure not to return a message that
  // is destined for a blocked actor! Otherwise an exception will be thrown.
  def schedule_new_message(blockedActors: Set[String]) : Option[(Cell, Envelope)]  
  // Get next event to schedule (used while restarting the system)
  def next_event() : Event
  // Notify that there are no more events to run
  def notify_quiescence () : Unit
  
  // Called before we start processing a newly received event
  def before_receive(cell: Cell) : Unit
  // Called after receive is done being processed 
  def after_receive(cell: Cell) : Unit
  
  def before_receive(cell: Cell, msg: Any) : Unit =
    before_receive(cell)
  def after_receive(cell: Cell, msg: Any) : Unit =
    after_receive(cell)
    
  // Record that an event was produced 
  def event_produced(event: Event) : Unit
  def event_produced(cell: Cell, envelope: Envelope) : Unit
  
  // Record that an event was consumed
  def event_consumed(event: Event) : Unit  
  def event_consumed(cell: Cell, envelope: Envelope)
  // Tell the scheduler that it should eventually schedule the given message.
  // Used to feed messages from the external world into actor systems.

   // Called when timer is cancelled
  def notify_timer_cancel(receiver: ActorRef, msg: Any)
  
  // Interface for (safely) sending external messages
  def enqueue_message(sender: Option[ActorRef], receiver: String, msg: Any)

  // Interface for (safely) sending timers (akka.scheduler messages)
  def enqueue_timer(receiver: String, msg: Any) = enqueue_message(None, receiver, msg)

  // Shut down the actor system.
  def shutdown()

}
