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

abstract class TraceEvent
final case class Start (prop: Props, name: String) extends TraceEvent
final case class Kill (name: String) extends TraceEvent {}
final case class Send (name: String, message: Any) extends TraceEvent
final case object WaitQuiescence extends TraceEvent
final case class Partition (a: String, b: String) extends TraceEvent
final case class UnPartition (a: String, b: String) extends TraceEvent

// Just a very simple, non-null scheduler that supports 
// partitions.
class TraceFairScheduler()
    extends FairScheduler {

  // Pairs of actors that cannot communicate
  val partitioned = new HashSet[(String, String)]
  
  // An actor that is unreachable
  val inaccessible = new HashSet[String]

  // Actors to actor ref
  val actorToActorRef = new HashMap[String, ActorRef]

  var trace: Array[TraceEvent] = Array()
  var traceIdx: Int = 0
  var events: Queue[Event] = new Queue[Event]() 
  val traceSem = new Semaphore(0)
  val peek = new AtomicBoolean(false)

  // A set of messages to send
  val messagesToSend = new HashSet[(ActorRef, Any)]
  
  // Ensure exactly one thread in the scheduler at a time
  val schedSemaphore = new Semaphore(1)

  // TODO: Find a way to make this safe/move this into instrumenter
  val initialActorSystem = ActorSystem("initialas", ConfigFactory.load())

  // Mark a couple of nodes as partitioned (so they cannot communicate)
  private[this] def add_to_partition (newly_partitioned: Set[(String, String)]) {
    partitioned ++= newly_partitioned
  }
  private[this] def add_to_partition (newly_partitioned: (String, String)) {
    partitioned += newly_partitioned
  }

  // Mark a couple of node as unpartitioned, i.e. they can communicate
  private[this] def remove_partition (newly_partitioned: Set[(String, String)]) {
    partitioned --= newly_partitioned
  }
  private[this] def remove_partition (newly_partitioned: (String, String)) {
    partitioned -= newly_partitioned
  }

  // Mark a node as unreachable, used to kill a node.
  private[this] def isolate_node (node: String) {
    inaccessible += node
  }

  // Mark a node as reachable, also used to start a node
  private[this] def unisolate_node (node: String) {
    inaccessible -= node
  }

  // Enqueue a message for future delivery
  private[this] def enqueue_message(receiver: String, msg: Any) : Boolean  = {
    if (!(actorNames contains receiver)) {
      false
    } else {
      enqueue_message(actorToActorRef(receiver), msg)
    }
  }

  private[this] def enqueue_message(actor: ActorRef, msg: Any) : Boolean  = {
    if (instrumenter.started.get()) { 
      messagesToSend += ((actor, msg))
      true
    } else {
      actor ! msg
      true
    }
  }
  

  // Given an external event trace, see the events produced
  def peek (_trace: Array[TraceEvent]) : Queue[Event]  = {
    trace = _trace
    events = new Queue[Event]()
    traceIdx = 0
    // We begin by starting all actors at the beginning of time, just mark them as 
    // isolated (i.e., unreachable)
    for (t <- trace) {
      t match {
        case Start (prop, name) => 
          // Just start and isolate all actors we might eventually care about
          initialActorSystem.actorOf(prop, name)
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
    println("play_trace done")
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
          unisolate_node(name)
        case Kill (name) =>
          isolate_node(name)
        case Send (name, message) =>
          val res = enqueue_message(name, message)
          require(res)
        case Partition (a, b) =>
          add_to_partition((a, b))
        case UnPartition (a, b) =>
          remove_partition((a, b))
        case WaitQuiescence =>
          loop = false // Start waiting for quiscence
      }
      traceIdx += 1
    }
    schedSemaphore.release
  }

  override def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])
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
    events += event
  }
  
  // Record a message send event
  override def event_consumed(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    events += MsgEvent(snd, rcv, msg, cell, envelope)
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
    if (traceIdx < trace.size) {
      // If waiting for quiescence.
      advanceTrace()
    } else {
      println("Done with events")
      if (peek.get) {
        // Tell the calling thread we are done
        traceSem.release
      }
    }
  }
}
