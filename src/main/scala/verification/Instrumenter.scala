package akka.dispatch.verification

import akka.actor.ActorCell
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.PoisonPill
import akka.actor.Props;
import akka.actor.Cancellable
import akka.cluster.VectorClock
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Semaphore
import java.io.Closeable

import akka.dispatch.Envelope
import akka.dispatch.MessageQueue
import akka.dispatch.MessageDispatcher

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Queue
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import scala.util.Random
import scala.util.control.Breaks

// Wrap cancellable so we get notified about cancellation
class WrappedCancellable (c: Cancellable, rcv: ActorRef, msg: Any) extends Cancellable {
  val instrumenter = Instrumenter()
  def cancel(): Boolean = {
    val success = c.cancel
    instrumenter.cancelTimer(this, rcv, msg, success)
    return success
  }

  def isCancelled: Boolean = c.isCancelled
}

class InstrumenterCheckpoint(
  val actorMappings : HashMap[String, ActorRef],
  val seenActors : HashSet[(ActorSystem, Any)],
  val allowedEvents: HashSet[(ActorCell, Envelope)],
  val dispatchers : HashMap[ActorRef, MessageDispatcher],
  val vectorClocks : HashMap[String, VectorClock],
  val sys: ActorSystem,
  var cancellableToTimer : HashMap[Cancellable, Tuple2[String, Any]],
  var ongoingCancellableTasks : HashSet[Cancellable],
  var timerToCancellable : HashMap[Tuple2[String,Any], Cancellable],
  // TODO(cs): add the random associated with this actor system
  val applicationCheckpoint: Any
) {}

class Instrumenter {
  // Provides the application a hook to compute after each shutdown
  type ShutdownCallback = () => Unit
  // Checkpoints the application state (outside the actor system)
  type CheckpointCallback = () => Any
  // Restores the application state.
  type RestoreCheckpointCallback = (Any) => Unit

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
  var shutdownCallback : ShutdownCallback = () => {}
  var checkpointCallback : CheckpointCallback = () => {null}
  var restoreCheckpointCallback : RestoreCheckpointCallback = (a: Any) => {}
  var registeredCancellableTasks = new HashSet[Cancellable]
  // Mark which of the timers are scheduleOnce vs schedule [ongoing]
  var ongoingCancellableTasks = new HashSet[Cancellable]
  // Track which cancellables correspond to which messages
  var cancellableToTimer = new HashMap[Cancellable, Tuple2[String, Any]]
  // And vice versa
  var timerToCancellable = new HashMap[Tuple2[String,Any], Cancellable]

  private[dispatch] def cancelTimer (c: Cancellable, rcv: ActorRef, msg: Any, success: Boolean) = {
    // Need this here since by the time DPORwHeuristics gets here the thing is already canceled
    if (cancellableToTimer contains c) {
      removeCancellable(c)
    }
    if (scheduler != null) {
      scheduler.notify_timer_cancel(rcv, msg)
    }
  }

  // AspectJ runs into initialization problems if a new ActorSystem is created
  // by the constructor. Instead use a getter to create on demand.
  private[this] var _actorSystem : ActorSystem = null
  def actorSystem () : ActorSystem = {
    if (_actorSystem == null) {
      _actorSystem = ActorSystem("new-system-" + counter)
      _random = new Random(0)
      counter += 1
    }
    _actorSystem
  }

  private[this] var _random = new Random(0)
  def seededRandom() : Random = {
    return _random
  }
 
  
  def await_enqueue() {
    tellEnqueue.await()
  }
  
  
  def tell(receiver: ActorRef, msg: Any, sender: ActorRef) : Unit = {
    if (!scheduler.isSystemCommunication(sender, receiver, msg)) {
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
    _random = new Random(0)
    counter += 1
    
    actorMappings.clear()
    seenActors.clear()
    allowedEvents.clear()
    dispatchers.clear()
    
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
  
  
  def shutdown_system(alsoRestart: Boolean) = {
    shutdownCallback()

    val allSystems = new HashMap[ActorSystem, Queue[Any]]
    for ((system, args) <- seenActors) {
      val argQueue = allSystems.getOrElse(system, new Queue[Any])
      argQueue.enqueue(args)
      allSystems(system) = argQueue
    }

    seenActors.clear()
    for ((system, argQueue) <- allSystems) {
      println("Shutting down the actor system. " + argQueue.size)
      if (alsoRestart) {
        system.registerOnTermination(reinitialize_system(system, argQueue))
      }
      for (task <- registeredCancellableTasks.filterNot(c => c.isCancelled)) {
        task.cancel()
      }
      registeredCancellableTasks.clear
      ongoingCancellableTasks.clear
      cancellableToTimer.clear
      timerToCancellable.clear

      system.shutdown()

      println("Shut down the actor system. " + argQueue.size)
    }

    Util.logger.reset
  }

  // Signal to the instrumenter that the scheduler wants to restart the system
  def restart_system() = {
    println("Restarting system")
    shutdown_system(true)
  }
  
  
    // Called before a message is received
  def beforeMessageReceive(cell: ActorCell) {
    throw new Exception("not implemented")
  }
  
  // Called after the message receive is done.
  def afterMessageReceive(cell: ActorCell) {
    throw new Exception("not implemented")
  }
  
  
  // Called before a message is received
  def beforeMessageReceive(cell: ActorCell, msg: Any) {
    if (scheduler.isSystemMessage(
        cell.sender.path.name, 
        cell.self.path.name,
        msg)) return
   
    scheduler.before_receive(cell, msg)
    currentActor = cell.self.path.name
    inActor = true
  }
  
  
  // Called after the message receive is done.
  def afterMessageReceive(cell: ActorCell, msg: Any) {
    if (scheduler.isSystemMessage(
        cell.sender.path.name, 
        cell.self.path.name,
        msg)) return

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
    val msg = envelope.message
    Util.logger.mergeVectorClocks(snd, rcv)

    
    allowedEvents += ((cell, envelope) : (ActorCell, Envelope))        

    val dispatcher = dispatchers.get(cell.self) match {
      case Some(value) => value
      case None => throw new Exception("internal error")
    }
    
    scheduler.event_consumed(cell, envelope)
    dispatcher.dispatch(cell, envelope)
    // Check if it was a repeating timer. If so, retrigger it.
    if (timerToCancellable contains (rcv, msg)) {
      val cancellable = timerToCancellable((rcv, msg))
      if (ongoingCancellableTasks contains cancellable) {
        println("Retriggering repeating timer: " + rcv + " " + msg)
        handleTick(cell.self, msg, cancellable)
      }
    }
  }
  
  
  // Called when dispatch is called.
  def aroundDispatch(dispatcher: MessageDispatcher, cell: ActorCell, 
      envelope: Envelope): Boolean = {
    val value: (ActorCell, Envelope) = (cell, envelope)
    val receiver = cell.self
    val snd = envelope.sender.path.name
    val rcv = receiver.path.name
    
    // If this is a system message just let it through.
    if (scheduler.isSystemMessage(snd, rcv, envelope.message)) { return true }
    
    // If this is not a system message then check if we have already recorded
    // this event. Recorded => we are injecting this event (as opposed to some 
    // actor doing it in which case we need to report it)
    if (allowedEvents contains value) {
      allowedEvents.remove(value) match {
        case true => return true
        case false => throw new Exception("internal error")
      }
    }
    
    // Record the dispatcher for the current receiver.
    dispatchers(receiver) = dispatcher

    // Record that this event was produced. The scheduler is responsible for 
    // kick starting processing.
    scheduler.event_produced(cell, envelope)
    tellEnqueue.enqueue()
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

  // When someone calls akka.actor.schedulerOnce to schedule a Timer, we
  // record the returned Cancellable object here, so that we can cancel it later.
  def registerCancellable(c: Cancellable, ongoingTimer: Boolean,
                          rcv: ActorRef, msg: Any) {
    val receiver = rcv.path.name
    registeredCancellableTasks += c
    if (ongoingTimer) {
      ongoingCancellableTasks += c
    }
    cancellableToTimer(c) = ((receiver, msg))
    // TODO(cs): for now, assume that msg's are unique. Don't assume that.
    if (timerToCancellable contains (receiver, msg)) {
      throw new RuntimeException("Non-unique timer: "+ receiver + " " + msg)
    }
    timerToCancellable((receiver, msg)) = c
    // Schedule it immediately!
    handleTick(rcv, msg, c)
  }

  def removeCancellable(c: Cancellable) {
    registeredCancellableTasks -= c
    val (receiver, msg) = cancellableToTimer(c)
    timerToCancellable -= ((receiver, msg))
    cancellableToTimer -= c
  }

  // When akka.actor.schedulerOnce decides to schedule a message to be sent,
  // we intercept it here.
  def handleTick(receiver: ActorRef, msg: Any, c: Cancellable) {
    // println("handleTick " + receiver + " " + msg)
    if (!(registeredCancellableTasks contains c)) {
      throw new IllegalArgumentException("Cancellable " + (receiver.path.name, msg) +
                                         "is already cancelled...")
    }
    scheduler.enqueue_timer(receiver.path.name, msg)
    if (!(ongoingCancellableTasks contains c)) {
      removeCancellable(c)
    }
  }

  def registerShutdownCallback(callback: ShutdownCallback) {
    shutdownCallback = callback
  }

  def registerCheckpointCallbacks(_applicationCheckpointCallback: CheckpointCallback,
                                  _restoreCheckpointCallback: RestoreCheckpointCallback) {
    checkpointCallback = _applicationCheckpointCallback
    restoreCheckpointCallback = _restoreCheckpointCallback
  }

  /**
   * If you're going to change out schedulers, call
   * `Instrumenter.scheduler = new_scheduler` *before* invoking this
   * method.
   */
  def checkpoint() : InstrumenterCheckpoint = {
    // TODO(cs): the shared state of the application is going to be reset on
    // reinitialize_system, due to shutdownCallback. This is problematic,
    // unless the application properly uses CheckpointSink's protocol for
    // checking invariants
    val checkpoint = new InstrumenterCheckpoint(
      new HashMap[String, ActorRef] ++ actorMappings,
      new HashSet[(ActorSystem, Any)] ++ seenActors,
      new HashSet[(ActorCell, Envelope)] ++ allowedEvents,
      new HashMap[ActorRef, MessageDispatcher] ++ dispatchers,
      new HashMap[String, VectorClock] ++ Util.logger.actor2vc,
      _actorSystem,
      new HashMap[Cancellable, Tuple2[String, Any]] ++ cancellableToTimer,
      new HashSet[Cancellable] ++ ongoingCancellableTasks,
      new HashMap[Tuple2[String,Any], Cancellable] ++ timerToCancellable,
      checkpointCallback()
    )

    Util.logger.reset

    // Reset all state so that a new actor system can be started.
    registeredCancellableTasks.clear
    ongoingCancellableTasks.clear
    cancellableToTimer.clear
    timerToCancellable.clear

    reinitialize_system(null, null)

    return checkpoint
  }

  /**
   * If you're going to change out schedulers, call
   * `Instrumenter.scheduler = old_scheduler` *before* invoking this
   * method.
   */
  def restoreCheckpoint(checkpoint: InstrumenterCheckpoint) {
    actorMappings.clear
    actorMappings ++= checkpoint.actorMappings
    seenActors.clear
    seenActors ++= checkpoint.seenActors
    allowedEvents.clear
    allowedEvents ++= checkpoint.allowedEvents
    dispatchers.clear
    dispatchers ++= checkpoint.dispatchers
    Util.logger.actor2vc = checkpoint.vectorClocks
    cancellableToTimer = checkpoint.cancellableToTimer
    ongoingCancellableTasks = checkpoint.ongoingCancellableTasks
    timerToCancellable = checkpoint.timerToCancellable
    registeredCancellableTasks.clear
    registeredCancellableTasks ++= cancellableToTimer.keys
    _actorSystem = checkpoint.sys
    restoreCheckpointCallback(checkpoint.applicationCheckpoint)
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
