package akka.dispatch.verification

import akka.actor.ActorCell
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.PoisonPill
import akka.actor.Props;
import java.util.concurrent.atomic.AtomicBoolean

import akka.dispatch.Envelope
import akka.dispatch.MessageQueue
import akka.dispatch.MessageDispatcher

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Queue
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.control.Breaks





class Instrumenter {
  var scheduler : Scheduler = new NullScheduler
  var tellEnqueue : TellEnqueue = new TellEnqueueSemaphore
  
  val dispatchers = new HashMap[ActorRef, MessageDispatcher]
  
  val allowedEvents = new HashSet[(ActorCell, Envelope)]  
  
  val seenActors = new HashSet[(ActorSystem, Any)]
  val actorMappings = new HashMap[String, ActorRef]
  
  // Track the executing context (i.e., source of events)
  var currentActor = ""
  var inActor = false
  var counter = 0   
  var started = new AtomicBoolean(false);

  // AspectJ runs into initialization problems if a new ActorSystem is created
  // by the constructor. Instead use a getter to create on demand.
  private[this] var _actorSystem : ActorSystem = null 
  def actorSystem () : ActorSystem = {
    if (_actorSystem == null) {
      _actorSystem = ActorSystem("new-system-" + counter)
      counter += 1
    }
    _actorSystem
  }
 
  def await_enqueue() {
    tellEnqueue.await()
  }
  
  def tell(receiver: ActorRef, msg: Any, sender: ActorRef) : Unit = {
    if (!scheduler.isSystemCommunication(sender, receiver)) {
      tellEnqueue.tell()
    }
  }
  
  
  // Callbacks for new actors being created
  def new_actor(system: ActorSystem, 
      props: Props, name: String, actor: ActorRef) : Unit = {
   
    val event = new SpawnEvent(currentActor, props, name, actor)
    scheduler.event_produced(event : SpawnEvent)
    scheduler.event_consumed(event)

    if (!started.get) {
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
    _actorSystem = ActorSystem("new-system-" + counter)
    counter += 1
    println("Started a new actor system.")
    // This is safe, we have just started a new actor system (after killing all
    // the old ones we knew about), there should be no actors running and no 
    // dispatch calls outstanding. That said, it is really important that we not
    // have other sources of ActorSystems etc.
    started.set(false)
    tellEnqueue.reset()

    // Tell scheduler that we are done restarting and it should prepare
    // to start the system
    scheduler.start_trace()
    // Rely on scheduler to do the right thing from here on out
  }
  
  
  // Signal to the instrumenter that the scheduler wants to restart the system
  def restart_system() = {
    
    println("Restarting system")
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
        println("Shut down the actor system. " + argQueue.size)
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

    tellEnqueue.await()
    
    inActor = false
    currentActor = ""
    scheduler.after_receive(cell)          
    scheduler.schedule_new_message() match {
      case Some((new_cell, envelope)) => dispatch_new_message(new_cell, envelope)
      case None =>
        counter += 1
        started.set(false)
        scheduler.notify_quiescence()
    }

  }

  // Dispatch a message, i.e., deliver it to the intended recipient
  def dispatch_new_message(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    Util.logger.mergeVectorClocks(snd, rcv)
    
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

    // Record that this event was produced. The scheduler is responsible for 
    // kick starting processing.
    scheduler.event_produced(cell, envelope)
    tellEnqueue.enqueue()
    //println(Console.BLUE +  "enqueue: " + snd + " -> " + rcv + Console.RESET);
    return false
  }
  
  // Start scheduling and dispatching messages. This makes the scheduler responsible for
  // actually kickstarting things. 
  def start_dispatch() {
    started.set(true)
    scheduler.schedule_new_message() match {
      case Some((new_cell, envelope)) => dispatch_new_message(new_cell, envelope)
      case None =>
        counter += 1
        started.set(false)
        scheduler.notify_quiescence()
    }
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
