package akka.dispatch.verification

import akka.actor.ActorCell
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.PoisonPill
import akka.actor.Props;

import akka.dispatch.Envelope
import akka.dispatch.MessageQueue
import akka.dispatch.MessageDispatcher

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.Iterator

import scala.collection.generic.GenericTraversableTemplate

// A basic scheduler. Schedules events in the order they arrive.
class BasicScheduler extends Scheduler {
  
  var instrumenter = Instrumenter()
  var currentTime = 0
  var index = 0
  
  type CurrentTimeQueueT = Queue[Event]
  
  var currentlyProduced = new CurrentTimeQueueT
  var currentlyConsumed = new CurrentTimeQueueT
  
  var producedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
  var consumedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
  
  var prevProducedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
  var prevConsumedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
 
  // Current set of enabled events.
  val pendingEvents = new HashMap[String, Queue[(ActorCell, Envelope)]]  

  val actorNames = new HashSet[String]
  
  def isSystemCommunication(sender: ActorRef, receiver: ActorRef): Boolean = {
    if (sender == null || receiver == null) return true
    return isSystemMessage(sender.path.name, receiver.path.name)
  }
  
  // Is this message a system message
  def isSystemMessage(src: String, dst: String): Boolean = {
    if ((actorNames contains src) || (actorNames contains dst))
      return false
    
    return true
  }
  
  
  // Notification that the system has been reset
  def start_trace() : Unit = {
    prevProducedEvents = producedEvents
    prevConsumedEvents = consumedEvents
    producedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
    consumedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
  }
  
  
  // When executing a trace, find the next trace event.
  private[this] def mutable_trace_iterator(
      trace: Queue[ (Integer, CurrentTimeQueueT) ]) : Option[Event] = { 
    
    if(trace.isEmpty) return None
      
    val (count, q) = trace.head
    q.isEmpty match {
      case true =>
        trace.dequeue()
        mutable_trace_iterator(trace)
      case false => return Some(q.dequeue())
    }
  }
  
  

  // Get next message event from the trace.
  private[this] def get_next_trace_message() : Option[MsgEvent] = {
    mutable_trace_iterator(prevConsumedEvents) match {
      case Some(v : MsgEvent) =>  Some(v)
      case Some(v : Event) => get_next_trace_message()
      case None => None
    }
  }
  
  
  
  // Figure out what is the next message to schedule.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
  
    // Filter for messages belong to a particular actor.
    def is_the_same(e: MsgEvent, c: (ActorCell, Envelope)) : Boolean = {
      val (cell, env) = c
      e.receiver == cell.self.path.name
    }

    // Get from the current set of pending events.
    def get_pending_event()  : Option[(ActorCell, Envelope)] = {
      // Do we have some pending events
      pendingEvents.headOption match {
        case Some((receiver, queue)) =>
           if (queue.isEmpty == true) {
             
             pendingEvents.remove(receiver) match {
               case Some(key) => get_pending_event()
               case None => throw new Exception("internal error")
             }
             
           } else {
              Some(queue.dequeue())
           }
        case None => None
      }
    }
    
    get_next_trace_message() match {
     // The trace says there is something to run.
     case Some(msg_event : MsgEvent) => 
       pendingEvents.get(msg_event.receiver) match {
         case Some(queue) => queue.dequeueFirst(is_the_same(msg_event, _))
         case None => None
       }
     // The trace says there is nothing to run so we have either exhausted our
     // trace or are running for the first time. Use any enabled transitions.
     case None => get_pending_event()
       
   }
  }
  
  
  // Get next event
  def next_event() : Event = {
    mutable_trace_iterator(prevConsumedEvents) match {
      case Some(v) => v
      case None => throw new Exception("no previously consumed events")
    }
  }
  

  // Record that an event was consumed
  def event_consumed(event: Event) = {
    currentlyConsumed.enqueue(event)
  }
  
  
  def event_consumed(cell: ActorCell, envelope: Envelope) = {
    currentlyConsumed.enqueue(new MsgEvent(
        envelope.sender.path.name, cell.self.path.name, envelope.message))
  }
  
  // Record that an event was produced 
  def event_produced(event: Event) = {
    event match {
      case event : SpawnEvent => actorNames += event.name
    }
    currentlyProduced.enqueue(event)
  }
  
  
  def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])
    
    pendingEvents(rcv) = msgs += ((cell, envelope))
    currentlyProduced.enqueue(new MsgEvent(snd, rcv, envelope.message))
    if (!instrumenter.started.get) {
      instrumenter.start_dispatch()
    }
  }
  
  
  // Called before we start processing a newly received event
  def before_receive(cell: ActorCell) {
    producedEvents.enqueue( (currentTime, currentlyProduced) )
    consumedEvents.enqueue( (currentTime, currentlyConsumed) )
    currentlyProduced = new CurrentTimeQueueT
    currentlyConsumed = new CurrentTimeQueueT
    currentTime += 1
    println(Console.GREEN 
        + " ↓↓↓↓↓↓↓↓↓ ⌚  " + currentTime + " | " + cell.self.path.name + " ↓↓↓↓↓↓↓↓↓ " + 
        Console.RESET)
  }
  
  
  // Called after receive is done being processed 
  def after_receive(cell: ActorCell) {
    println(Console.RED 
        + " ↑↑↑↑↑↑↑↑↑ ⌚  " + currentTime + " | " + cell.self.path.name + " ↑↑↑↑↑↑↑↑↑ " 
        + Console.RESET)
        
  }
  

  def notify_quiescence () {
  }
  
  def enqueue_message(receiver: String, msg: Any) {
    throw new Exception("NYI")
  }

  def shutdown() {
    instrumenter.restart_system
  }

}
