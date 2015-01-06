package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean



/**
 * Takes a sequence of ExternalEvents as input, and plays the execution
 * forward in the same way as FairScheduler. While playing forward,
 * PeekScheduler records all internal events that occur, e.g. Message Sends.
 * PeekScheduler finally returns all events observed during the execution, including
 * external and internal events.
 */
class PeekScheduler()
    extends FairScheduler with TestOracle {

  // Pairs of actors that cannot communicate
  val partitioned = new HashSet[(String, String)]
  
  // Actors that are unreachable
  val inaccessible = new HashSet[String]

  // Actors to actor ref
  val actorToActorRef = new HashMap[String, ActorRef]
  val actorToSpawnEvent = new HashMap[String, SpawnEvent]

  private[this] var trace: Seq[ExternalEvent] = new Array[ExternalEvent](0)
  private[this] var traceIdx: Int = 0
  var events: Queue[Event] = new Queue[Event]() 
  
  // Semaphore to wait for trace replay to be done. We initialize the
  // semaphore to 0 rather than 1, so that the main thread blocks upon
  // invoking acquire() until another thread release()'s it.
  private[this] val traceSem = new Semaphore(0)

  // Are we in peek or is someone using this scheduler in some strange way.
  private[this] val peek = new AtomicBoolean(false)

  // Semaphore to use when shutting down the scheduler. We initialize the
  // semaphore to 0 rather than 1, so that the main thread blocks upon
  // invoking acquire() until another thread release()'s it.
  private[this] val shutdownSem = new Semaphore(0)

  // A set of external messages to send. Messages sent between actors are not
  // queued here.
  val messagesToSend = new SynchronizedQueue[(ActorRef, Any)]()

  // Are we expecting message receives
  private[this] val started = new AtomicBoolean(false)

  // Ensure that only one thread is running inside the scheduler when we are
  // dispatching external messages to actors. (Does not guard the scheduler's instance
  // variables.)
  private[this] val schedSemaphore = new Semaphore(1)

  // Handler for FailureDetector messages
  val fd = new FDMessageOrchestrator(this)

  var test_invariant : Invariant = null

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
    fd.isolate_node(node)
  }

  // Mark a node as reachable, also used to start a node
  private[this] def unisolate_node (actor: String) {
    inaccessible -= actor
    fd.unisolate_node(actor)
  }

  // Enqueue a message for future delivery
  override def enqueue_message(receiver: String, msg: Any) {
    if (actorNames contains receiver) {
      enqueue_message(actorToActorRef(receiver), msg)
    } else {
      throw new IllegalArgumentException("Unknown receiver " + receiver)
    }
  }

  private[this] def enqueue_message(actor: ActorRef, msg: Any) {
    messagesToSend += ((actor, msg))
  }

  // Given an external event trace, see the events produced
  def peek (_trace: Seq[ExternalEvent]) : Queue[Event]  = {
    trace = _trace
    events = new Queue[Event]()
    traceIdx = 0
    fd.startFD(instrumenter.actorSystem)
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
          fd.handle_start_event(name)
        case Kill (name) =>
          Util.logger.log(name, "God killed me")
          events += KillEvent(name)
          isolate_node(name)
          fd.handle_kill_event(name)
        case Send (name, message) =>
          enqueue_message(name, message)
        case Partition (a, b) =>
          events += PartitionEvent((a, b))
          add_to_partition((a, b))
          Util.logger.log(a, "God partitioned me from " + b)
          Util.logger.log(b, "God partitioned me from " + a)
          fd.handle_partition_event(a,b)
        case UnPartition (a, b) =>
          events += UnPartitionEvent((a, b))
          remove_partition((a, b))
          Util.logger.log(a, "God reconnected me to " + b)
          Util.logger.log(b, "God reconnected me to " + a)
          fd.handle_unpartition_event(a,b)
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
    if (rcv == FailureDetector.fdName) {
      fd.handle_fd_message(envelope.message, snd)
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
        fd.create_node(event.name)
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
    
    // Send all pending fd responses
    fd.send_all_pending_responses()
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
      } else {
        throw new RuntimeException("peek.get returned false")
      }
    }
  }

  // Shutdown the scheduler, this ensures that the instrumenter is returned to its
  // original pristine form, so one can change schedulers
  override def shutdown () = {
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

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }

  def test(events: Seq[ExternalEvent]) : Boolean = {
    peek(events)
    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }
    val passes = test_invariant(events)
    shutdown()
    return passes
  }
}
