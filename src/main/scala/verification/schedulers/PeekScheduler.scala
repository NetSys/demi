package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

// A fake actor, used as a placeholder to which failure detector requests can be sent.
class FailureDetector () extends Actor {
  def receive = {
    // This should never be called
    case _ => assert(false)
  }
}

// Just a very simple, non-null scheduler that supports 
// partitions and injecting external events.
class PeekScheduler()
    extends FairScheduler {

  // Name of the failure detector actor
  val fdName = "failure_detector"

  // Pairs of actors that cannot communicate
  val partitioned = new HashSet[(String, String)]
  
  // Actors that are unreachable
  val inaccessible = new HashSet[String]

  // Failure detector information
  // For each actor, track the set of other actors who are reachable.
  val fdState = new HashMap[String, HashSet[String]]
  val activeActors = new HashSet[String]
  val pendingFDRequests = new Queue[String]

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

  // Are we expecting message receives
  private[this] val started = new AtomicBoolean(false)

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
  // TODO(cs): to be implemented later: actually kill the node so that its state is cleared?
  private[this] def isolate_node (node: String) {
    inaccessible += node
    fdState(node).clear()
  }

  // Mark a node as reachable, also used to start a node
  private[this] def unisolate_node (actor: String) {
    inaccessible -= actor
    fdState(actor) ++= activeActors
  }

  // Enqueue a message for future delivery
  private[this] def enqueue_message(receiver: String, msg: Any) {
    if (actorNames contains receiver) {
      enqueue_message(actorToActorRef(receiver), msg)
    }
  }

  private[this] def enqueue_message(actor: ActorRef, msg: Any) {
    messagesToSend += ((actor, msg))
  }

  // Given an external event trace, see the events produced
  def peek (_trace: Array[ExternalEvent]) : Queue[Event]  = {
    trace = _trace
    events = new Queue[Event]()
    traceIdx = 0
    instrumenter.actorSystem.actorOf(Props[FailureDetector], fdName)
    // We begin by starting all actors at the beginning of time, just mark them as 
    // isolated (i.e., unreachable). Later, when we replay the `Start` event,
    // we unisolate the actor.
    for (t <- trace) {
      t match {
        case Start (prop, name) => 
          // Just start and isolate all actors we might eventually care about [top-level actors]
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

  private[this] def informFailureDetectorLocation(actor: String) {
    enqueue_message(actor, FailureDetectorOnline(fdName))
  }

  private[this] def informNodeReachable(actor: String, node: String) {
   enqueue_message(actor, NodeReachable(node))
   fdState(actor) += node
  }

  private[this] def informNodeReachable(node: String) {
    for (actor <- activeActors) {
      informNodeReachable(actor, node)
    }
  }

  private[this] def informNodeUnreachable(actor: String, node: String) {
    enqueue_message(actor, NodeUnreachable(node))
    fdState(actor) -= node
  }

  private[this] def informNodeUnreachable(node: String) {
    for (actor <- activeActors) {
      informNodeUnreachable(actor, node)
    }
  }

  private[this] def answerFdQuery(sender: String) {
    // Compute the message
    val msg = ReachableGroup(fdState(sender).toArray)
    // Send failure detector information
    enqueue_message(sender, msg)
  }

  // Advance the trace
  private[this] def advanceTrace() {
    // Make sure the actual scheduler makes no progress until we have injected all
    // events. 
    schedSemaphore.acquire
    started.set(true)
    var loop = true
    while (loop && traceIdx < trace.size) {
      // TODO(cs): factor this code out. Currently redundant with ReplayScheduler's advanceTrace().
      trace(traceIdx) match {
        case Start (_, name) =>
          Util.logger.log(name, "God spawned me")
          events += actorToSpawnEvent(name)
          unisolate_node(name)
          // Send FD message before adding an actor
          informFailureDetectorLocation(name)
          informNodeReachable(name)
          activeActors += name
        case Kill (name) =>
          Util.logger.log(name, "God killed me")
          events += KillEvent(name)
          isolate_node(name)
          activeActors -= name
          // Send FD message after removing the actor
          informNodeUnreachable(name)
        case Send (name, message) =>
          enqueue_message(name, message)
        case Partition (a, b) =>
          events += PartitionEvent((a, b))
          add_to_partition((a, b))
          Util.logger.log(a, "God partitioned me from " + b)
          Util.logger.log(b, "God partitioned me from " + a)
          // Send FD information to each of the actors
          informNodeUnreachable(a, b)
          informNodeUnreachable(b, a)
        case UnPartition (a, b) =>
          events += UnPartitionEvent((a, b))
          remove_partition((a, b))
          Util.logger.log(a, "God reconnected me to " + b)
          Util.logger.log(b, "God reconnected me to " + a)
          // Send FD information to each of the actors
          informNodeReachable(a, b)
          informNodeReachable(b, a)
        case WaitQuiescence =>
          loop = false // Start waiting for quiescence
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
    events += MsgSend(snd, rcv, envelope.message) 
    // Intercept any messages sent towards the failure detector
    if (rcv == fdName) {
      envelope.message match {
        case QueryReachableGroup =>
          // Allow queries to be made during class initialization (more than one might be happening at
          // a time)
          pendingFDRequests += snd
        case _ =>
          assert(false)
      }
    } else if (!((partitioned contains (snd, rcv)) // Drop any messages that crosses a partition.
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
        fdState(event.name) = new HashSet[String]
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
    assert(started.get)
    events += ChangeContext(cell.self.path.name)
    events += MsgEvent(snd, rcv, msg)
  }

  override def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    // Ensure that only one thread is accessing shared scheduler structures
    schedSemaphore.acquire
    assert(started.get)
    
    // Send all waiting fd responses
    for (receiver <- pendingFDRequests) {
      answerFdQuery(receiver)
    }
    pendingFDRequests.clear
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
    assert(started.get)
    started.set(false)
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
  }
  override def after_receive(cell: ActorCell) : Unit = {
    events += ChangeContext("scheduler")
  }
}
