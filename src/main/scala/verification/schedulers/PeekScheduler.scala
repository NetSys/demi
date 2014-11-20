package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

// Just a very simple, non-null scheduler that supports 
// partitions and injecting external events.
class PeekScheduler()
    extends FairScheduler {

  // Pairs of actors that cannot communicate
  val partitioned = new HashSet[(String, String)]
  
  // An actor that is unreachable
  val inaccessible = new HashSet[String]

  // Actors to actor ref
  val actorToActorRef = new HashMap[String, ActorRef]
  val actorToSpawnEvent = new HashMap[String, SpawnEvent]

  private[this] var trace: Array[ExternalEvent] = Array()
  private[this] var traceIdx: Int = 0
  var events: Queue[Event] = new Queue[Event]() 
  
  // Semaphore to wait for trace replay to be done 
  private[this] val traceSem = new Semaphore(0)

  // Are we in peek or is someone using this scheduler in some strange way.
  private[this] val peek = new AtomicBoolean(false)

  // Semaphore to use when shutting down the scheduler
  private[this] val shutdownSem = new Semaphore(0)

  // A set of messages to send
  val messagesToSend = new HashSet[(ActorRef, Any)]
  
  // Ensure exactly one thread in the scheduler at a time
  private[this] val schedSemaphore = new Semaphore(1)


  // Mark a couple of nodes as partitioned (so they cannot communicate)
  private[this] def add_to_partition (newly_partitioned: (String, String)) {
    partitioned += newly_partitioned
  }

  // Mark a couple of node as unpartitioned, i.e. they can communicate
  private[this] def remove_partition (newly_partitioned: (String, String)) {
    partitioned -= newly_partitioned
  }

  // Mark a node as unreachable, used to kill a node.
  private[this] def isolate_node (node: String) {
    inaccessible += node
  }

  // Mark a node as reachable, also used to start a node
  private[this] def unisolate_node (actor: String) {
    inaccessible -= actor
  }

  // Enqueue a message for future delivery
  private[this] def enqueue_message(receiver: String, msg: Any) {
    if (actorNames contains receiver) {
      enqueue_message(actorToActorRef(receiver), msg)
    }
  }

  private[this] def enqueue_message(actor: ActorRef, msg: Any) {
    if (instrumenter.started.get()) { 
      messagesToSend += ((actor, msg))
    } else {
      actor ! msg
    }
  }

  // Given an external event trace, see the events produced
  def peek (_trace: Array[ExternalEvent]) : Queue[Event]  = {
    trace = _trace
    events = new Queue[Event]()
    traceIdx = 0
    // We begin by starting all actors at the beginning of time, just mark them as 
    // isolated (i.e., unreachable)
    for (t <- trace) {
      t match {
        case Start (prop, name) => 
          // Just start and isolate all actors we might eventually care about
          instrumenter.actorSystem.actorOf(prop, name)
          isolate_node(name)
        case _ =>
          None
      }
    }
    peek.set(true)
    // Start playing back trace
    advanceTrace()
    // Have this thread wait until the trace is down. This allows us to safely notify
    // the caller.
    traceSem.acquire
    peek.set(false)
    return events
  }

  // Advance the trace
  private[this] def advanceTrace() {
    // Make sure the actual scheduler makes no progress until we have injected all
    // events. 
    schedSemaphore.acquire
    var loop = true
    while (loop && traceIdx < trace.size) {
      trace(traceIdx) match {
        case Start (_, name) =>
          events += actorToSpawnEvent(name)
          unisolate_node(name)
        case Kill (name) =>
          events += KillEvent(name)
          isolate_node(name)
        case Send (name, message) =>
          enqueue_message(name, message)
        case Partition (a, b) =>
          events += PartitionEvent((a, b))
          add_to_partition((a, b))
        case UnPartition (a, b) =>
          events += UnPartitionEvent((a, b))
          remove_partition((a, b))
        case WaitQuiescence =>
          loop = false // Start waiting for quiscence
      }
      traceIdx += 1
    }
    schedSemaphore.release
    // Since this is always called during quiescence, once we have processed all 
    // events, let us start dispatching
    instrumenter.start_dispatch()
  }

  override def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])
    //println("Peek Send " + (snd, rcv, envelope.message))
    events += MsgSend(snd, rcv, envelope.message) 
    // Drop any messages that crosses a partition.
    if (!((partitioned contains (snd, rcv)) 
         || (partitioned contains (rcv, snd))
         || (inaccessible contains rcv) 
         || (inaccessible contains snd))) {
      pendingEvents(rcv) = msgs += ((cell, envelope))
    }
  }

  // Record a mapping from actor names to actor refs
  override def event_produced(event: Event) = {
    super.event_produced(event)
    event match { 
      case event : SpawnEvent => 
        actorToActorRef(event.name) = event.actor
    }
  }

  // Record that an event was consumed
  override def event_consumed(event: Event) = {
    val spawn_event = event.asInstanceOf[SpawnEvent]
    actorToSpawnEvent(spawn_event.name) = spawn_event
  }
  
  // Record a message send event
  override def event_consumed(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    events += MsgEvent(snd, rcv, msg)
  }

  override def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    // Ensure that only one thread is accessing shared scheduler structures
    schedSemaphore.acquire
    
    // Drain message queue 
    for ((receiver, msg) <- messagesToSend) {
      receiver ! msg
    }
    messagesToSend.clear()

    // Wait to make sure all messages are enqueued
    instrumenter.await_enqueue()

    // schedule_new_message is reenterant, hence release before calling.
    schedSemaphore.release
    super.schedule_new_message()
  }

  override def notify_quiescence () {
    events += Quiescence
    if (traceIdx < trace.size) {
      // If waiting for quiescence.
      advanceTrace()
    } else {
      if (peek.get) {
        // Tell the calling thread we are done
        traceSem.release
      }
    }
  }

  // Shutdown the scheduler, this ensures that the instrumenter is returned to its
  // original pristine form, so one can change schedulers
  def shutdown () = {
    instrumenter.restart_system
    shutdownSem.acquire
  }
  
  // Notification that the system has been reset
  override def start_trace() : Unit = {
    shutdownSem.release
  }

  override def before_receive(cell: ActorCell) : Unit = {
    events += ChangeContext(cell.self.path.name)
  }
  override def after_receive(cell: ActorCell) : Unit = {
    events += ChangeContext("scheduler")
  }
}
