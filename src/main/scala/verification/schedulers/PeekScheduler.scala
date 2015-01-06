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

  private[this] val event_orchestrator = new EventOrchestrator[ExternalEvent]()

  // Handler for FailureDetector messages
  val fd = new FDMessageOrchestrator(this)
  event_orchestrator.set_failure_detector(fd)
  
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

  var test_invariant : Invariant = null

  // Enqueue a message for future delivery
  override def enqueue_message(receiver: String, msg: Any) {
    if (actorNames contains receiver) {
      enqueue_message(event_orchestrator.actorToActorRef(receiver), msg)
    } else {
      throw new IllegalArgumentException("Unknown receiver " + receiver)
    }
  }

  private[this] def enqueue_message(actor: ActorRef, msg: Any) {
    messagesToSend += ((actor, msg))
  }

  // Given an external event trace, see the events produced
  def peek (_trace: Seq[ExternalEvent]) : Queue[Event]  = {
    event_orchestrator.set_trace(_trace)
    fd.startFD(instrumenter.actorSystem)
    // We begin by starting all actors at the beginning of time, just mark them as
    // isolated (i.e., unreachable). Later, when we replay the `Start` event,
    // we unisolate the actor.
    for (t <- event_orchestrator.trace) {
      t match {
        case Start (prop, name) => 
          // Just start and isolate all actors we might eventually care about [top-level actors]
          instrumenter.actorSystem.actorOf(prop, name)
          event_orchestrator.isolate_node(name)
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
    return event_orchestrator.events
  }

  // Advance the trace
  private[this] def advanceTrace() {
    // Make sure the actual scheduler makes no progress until we have injected all
    // events. 
    schedSemaphore.acquire
    started.set(true)
    event_orchestrator.inject_until_quiescence(enqueue_message)
    schedSemaphore.release
    // Since this is always called during quiescence, once we have processed all 
    // events, let us start dispatching
    instrumenter.start_dispatch()
  }

  override def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])
    event_orchestrator.events += MsgSend(snd, rcv, envelope.message)
    // Intercept any messages sent towards the failure detector
    if (rcv == FailureDetector.fdName) {
      fd.handle_fd_message(envelope.message, snd)
    // Drop any messages that crosses a partition.
    } else if (!event_orchestrator.crosses_partition(snd, rcv)) {
      pendingEvents(rcv) = msgs += ((cell, envelope))
    }
  }

  // Record a mapping from actor names to actor refs
  override def event_produced(event: Event) = {
    super.event_produced(event)
    event match { 
      case event : SpawnEvent => 
        event_orchestrator.handle_spawn_produced(event)
    }
  }

  // Record that an event was consumed
  override def event_consumed(event: Event) = {
    val spawn_event = event.asInstanceOf[SpawnEvent]
    event_orchestrator.handle_spawn_consumed(spawn_event)
  }
  
  // Record a message send event
  override def event_consumed(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    assert(started.get)
    event_orchestrator.events += ChangeContext(cell.self.path.name)
    event_orchestrator.events += MsgEvent(snd, rcv, msg)
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
    event_orchestrator.events += Quiescence
    if (!event_orchestrator.trace_finished) {
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
    event_orchestrator.events += ChangeContext("scheduler")
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
