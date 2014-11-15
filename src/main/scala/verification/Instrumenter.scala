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
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.control.Breaks

import java.util.concurrent.atomic.AtomicInteger



abstract class Event

case class MsgEvent(sender: String, receiver: String, msg: Any, 
               cell: ActorCell, envelope: Envelope) extends Event

case class SpawnEvent(parent: String,
    props: Props, name: String, actor: ActorRef) extends Event



class Instrumenter {

  var scheduler : Scheduler = new NullScheduler
  val dispatchers = new HashMap[ActorRef, MessageDispatcher]
  
  val allowedEvents = new HashSet[(ActorCell, Envelope)]  
  
  val seenActors = new HashSet[(ActorSystem, Any)]
  val actorMappings = new HashMap[String, ActorRef]
  
  // Track the executing context (i.e., source of events)
  var currentActor = ""
  var inActor = false
  var counter = 0   
  var started = false;
    
  var enqueue_count = new AtomicInteger
  var tell_count = new AtomicInteger

  
  
  def tell(receiver: ActorRef, msg: Any, sender: ActorRef) : Unit = {
    if (!scheduler.isSystemCommunication(sender, receiver))
      tell_count.incrementAndGet()
  }
  
  
  def wait_for_enqueue() = {
    while (tell_count.get != enqueue_count.get) {}
  }
  
  
  // Callbacks for new actors being created
  def new_actor(system: ActorSystem, 
      props: Props, name: String, actor: ActorRef) : Unit = {
    
    val event = new SpawnEvent(currentActor, props, name, actor)
    scheduler.event_produced(event : SpawnEvent)
    scheduler.event_consumed(event)

    if (!started) {
      seenActors += ((system, (actor, props, name)))
    }
    
    actorMappings(name) = actor
      
    println("System has created a new actor: " + actor.path.name)
  }
  
  
  def new_actor(system: ActorSystem, 
      props: Props, actor: ActorRef) : Unit = {
    new_actor(system, props, actor.path.name, actor)
  }
  
  
  // Restart the system:
  //  - Create a new actor system
  //  - Inform the scheduler that things have been reset
  //  - Run the first event to start the first actor
  //  - Send the first message received by this actor.
  //  This is all assuming that we don't control replay of main
  //  so this is a way to replay the first message that started it all.
  def reinitialize_system(sys: ActorSystem, argQueue: Queue[Any]) {
    require(scheduler != null)
    val newSystem = ActorSystem("new-system-" + counter)
    counter += 1
    println("Started a new actor system.")

    // Tell scheduler that we are done restarting and it should prepare
    // to start the system
    // TODO: This should probably take sys as an argument or something
    scheduler.start_trace()
    
    // We expect the first event to be an actor spawn (not actors exist, nothing
    // to run).
    val first_spawn = scheduler.next_event() match {
      case e: SpawnEvent => e
      case _ => throw new Exception("not a spawn")
    }
    
    // Start the actor using a new actor system. (All subsequent actors
    // are expected to spawn from here, so they will automatically inherit
    // the new actor system)
    for (args <- argQueue) {
      args match {
        case (actor: ActorRef, props: Props, first_spawn.name) =>
          println("starting " + first_spawn.name)
          newSystem.actorOf(props, first_spawn.name)
      }
    }

    // TODO: Maybe we should do this differently (the same way we inject external
    // events, etc.)
    // Kick off the system by replaying a message
    val first_msg = scheduler.next_event() match {
      case e: MsgEvent => e
      case _ => throw new Exception("not a message")
    }
    
    actorMappings.get(first_msg.receiver) match {
      case Some(ref) => ref ! first_msg.msg
      case None => throw new Exception("no such actor " + first_msg.receiver)
    }
  }
  
  
  // Signal to the instrumenter that the scheduler wants to restart the system
  def restart_system() = {
    
    println("Restarting system")
    
    started = false
    enqueue_count.set(0)
    tell_count.set(0)
    
    val allSystems = new HashMap[ActorSystem, Queue[Any]]
    for ((system, args) <- seenActors) {
      val argQueue = allSystems.getOrElse(system, new Queue[Any])
      argQueue.enqueue(args)
      allSystems(system) = argQueue
    }

    seenActors.clear()
    for ((system, argQueue) <- allSystems) {
        println("Shutting down the actor system. " + argQueue.size)
        system.shutdown()
        system.registerOnTermination(reinitialize_system(system, argQueue))
    }
  }
  
  
  // Called before a message is received
  def beforeMessageReceive(cell: ActorCell) {
    
    if (scheduler.isSystemMessage(cell.sender.path.name, cell.self.path.name)) return
   
    scheduler.before_receive(cell)
    currentActor = cell.self.path.name
    inActor = true
  }
  
  
  // Called after the message receive is done.
  def afterMessageReceive(cell: ActorCell) {
    if (scheduler.isSystemMessage(cell.sender.path.name, cell.self.path.name)) return

    wait_for_enqueue()
    
    inActor = false
    currentActor = ""
    scheduler.after_receive(cell)          
    scheduler.schedule_new_message() match {
      case Some((new_cell, envelope)) => dispatch_new_message(new_cell, envelope)
      case None =>
        counter += 1
        println("Nothing to run.")
        started = false
        scheduler.notify_quiescence()
    }

  }

  // Dispatch a message, i.e., deliver it to the intended recipient
  def dispatch_new_message(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    
    allowedEvents += ((cell, envelope) : (ActorCell, Envelope))        

    val dispatcher = dispatchers.get(cell.self) match {
      case Some(value) => value
      case None => throw new Exception("internal error")
    }
    
    scheduler.event_consumed(cell, envelope)
    dispatcher.dispatch(cell, envelope)
  }
  
  
  // Called when dispatch is called.
  def aroundDispatch(dispatcher: MessageDispatcher, cell: ActorCell, 
      envelope: Envelope): Boolean = {

    val value: (ActorCell, Envelope) = (cell, envelope)
    val receiver = cell.self
    val snd = envelope.sender.path.name
    val rcv = receiver.path.name
    
    // If this is a system message just let it through.
    if (scheduler.isSystemMessage(snd, rcv)) { return true }
    
    // If this is not a system message then check if we have already recorded
    // this event. Recorded => we are injecting this event (as opposed to some 
    // actor doing it in which case we need to report it)
    if (allowedEvents contains value) {
      allowedEvents.remove(value) match {
        case true => 
          return true
        case false => throw new Exception("internal error")
      }
    }
    
    // Record the dispatcher for the current receiver.
    dispatchers(receiver) = dispatcher

    // Have we started dispatching messages (i.e., is the loop in after_message_receive
    // running?). If not then dispatch the current message and start the loop.
    if (!started) {
      started = true
      tell_count.set(0)
      dispatch_new_message(cell, envelope)
      return false
    }
    // Record that this event was produced
    scheduler.event_produced(cell, envelope)
    enqueue_count.incrementAndGet()

    
    println(Console.BLUE +  "enqueue: " + snd + " -> " + rcv + Console.RESET);
    require(inActor)

    return false
  }

}

object Instrumenter {
  var obj:Instrumenter = null
  def apply() = {
    if (obj == null) {
      obj = new Instrumenter
    }
    obj
  }
}
