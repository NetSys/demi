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

abstract class MessageType()
final case object ExternalMessage extends MessageType
final case object InternalMessage extends MessageType
final case object SystemMessage extends MessageType

// Provides O(1) lookup, but allows multiple distinct elements
class MultiSet[E] {
  var m = new HashMap[E, List[E]]

  def add(e: E) = {
    if (m.contains(e)) {
      m(e) = e :: m(e)
    } else {
      m(e) = List(e)
    }
  }

  def contains(e: E) : Boolean = {
    return m.contains(e)
  }

  def remove(e: E) = {
    if (!m.contains(e)) {
      throw new IllegalArgumentException("No such element " + e)
    }
    m(e) = m(e).tail
    if (m(e).isEmpty) {
      m -= e
    }
  }
}

/**
 * A mix-in for schedulers that take external events as input, and generate
 * executions containing both external and internal events as output.
 */
trait ExternalEventInjector {

  var event_orchestrator = new EventOrchestrator[ExternalEvent]()

  // Handler for FailureDetector messages
  var fd = new FDMessageOrchestrator(enqueue_message)
  event_orchestrator.set_failure_detector(fd)

  // Semaphore to wait for trace replay to be done. We initialize the
  // semaphore to 0 rather than 1, so that the main thread blocks upon
  // invoking acquire() until another thread release()'s it.
  var traceSem = new Semaphore(0)

  // Are we currently injecting external events or is someone using
  // this scheduler in some strange way.
  val currentlyInjecting = new AtomicBoolean(false)

  // Semaphore to use when shutting down the scheduler. We initialize the
  // semaphore to 0 rather than 1, so that the main thread blocks upon
  // invoking acquire() until another thread release()'s it.
  var shutdownSem = new Semaphore(0)

  // A set of external messages to send. Messages sent between actors are not
  // queued here.
  var messagesToSend = new SynchronizedQueue[(ActorRef, Any)]()

  // Are we expecting message receives
  val started = new AtomicBoolean(false)

  // Ensure that only one thread is running inside the scheduler when we are
  // dispatching external messages to actors. (Does not guard the scheduler's instance
  // variables.)
  var schedSemaphore = new Semaphore(1)

  // If we enqueued an external message, keep track of it, so that we can
  // later identify it as an external message when it is plumbed through
  // event_produced
  val enqueuedExternalMessages = new MultiSet[Any]

  // Enqueue an external message for future delivery
  def enqueue_message(receiver: String, msg: Any) {
    if (event_orchestrator.actorToActorRef contains receiver) {
      enqueue_message(event_orchestrator.actorToActorRef(receiver), msg)
    } else {
      throw new IllegalArgumentException("Unknown receiver " + receiver)
    }
  }

  def enqueue_message(actor: ActorRef, msg: Any) {
    enqueuedExternalMessages.add(msg)
    messagesToSend += ((actor, msg))
  }

  // Initiates message sends for all messages in messagesToSend. Note that
  // delivery does not occur immediately! These messages will subsequently show
  // up in event_produced as messages to be scheduled by schedule_new_message.
  def enqueue_external_messages() {
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
    Instrumenter().await_enqueue()

    // schedule_new_message is reenterant, hence release before calling.
    schedSemaphore.release
  }

  // Given an external event trace, see the events produced
  def execute_trace (_trace: Seq[ExternalEvent]) : Queue[Event]  = {
    event_orchestrator.set_trace(_trace)
    fd.startFD(Instrumenter().actorSystem)
    // We begin by starting all actors at the beginning of time, just mark them as
    // isolated (i.e., unreachable). Later, when we replay the `Start` event,
    // we unisolate the actor.
    for (t <- event_orchestrator.trace) {
      t match {
        case Start (prop, name) => 
          // Just start and isolate all actors we might eventually care about [top-level actors]
          Instrumenter().actorSystem.actorOf(prop, name)
          event_orchestrator.isolate_node(name)
        case _ =>
          None
      }
    }
    currentlyInjecting.set(true)
    // Start playing back trace
    advanceTrace()
    // Have this thread wait until the trace is down. This allows us to safely notify
    // the caller.
    traceSem.acquire
    currentlyInjecting.set(false)
    return event_orchestrator.events
  }

  // Advance the trace
  def advanceTrace() {
    // Make sure the actual scheduler makes no progress until we have injected all
    // events.
    schedSemaphore.acquire
    started.set(true)
    event_orchestrator.inject_until_quiescence(enqueue_message)
    schedSemaphore.release
    // Since this is always called during quiescence, once we have processed all
    // events, let us start dispatching
    Instrumenter().start_dispatch()
  }

  /**
   * Return `Internal` object if the event is an internal event,
   * `External` if the event is an externally triggered messages destined
   * towards an actor, else `System` if it is not destined towards an actor.
   */
  def handle_event_produced(snd: String, rcv: String, envelope: Envelope) : MessageType = {
    event_orchestrator.events += MsgSend(snd, rcv, envelope.message)
    // Intercept any messages sent towards the failure detector
    if (rcv == FailureDetector.fdName) {
      fd.handle_fd_message(envelope.message, snd)
      return SystemMessage
    } else if (enqueuedExternalMessages.contains(envelope.message)) {
      return ExternalMessage
    } else {
      return InternalMessage
    }
  }

  def crosses_partition(snd: String, rcv: String) : Boolean = {
    return event_orchestrator.crosses_partition(snd, rcv)
  }

  def handle_spawn_produced(event: Event) {
    event match {
      case event : SpawnEvent =>
        event_orchestrator.handle_spawn_produced(event)
    }
  }

  def handle_spawn_consumed(event: Event) {
    val spawn_event = event.asInstanceOf[SpawnEvent]
    event_orchestrator.handle_spawn_consumed(spawn_event)
  }

  def handle_event_consumed(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    if (enqueuedExternalMessages.contains(msg)) {
      enqueuedExternalMessages.remove(msg)
    }
    assert(started.get)
    event_orchestrator.events += ChangeContext(cell.self.path.name)
    event_orchestrator.events += MsgEvent(snd, rcv, msg)
  }

  def handle_quiescence () {
    assert(started.get)
    started.set(false)
    event_orchestrator.events += Quiescence
    if (!event_orchestrator.trace_finished) {
      // If waiting for quiescence.
      advanceTrace()
    } else {
      if (currentlyInjecting.get) {
        // Tell the calling thread we are done
        traceSem.release
      } else {
        throw new RuntimeException("exploring.get returned false")
      }
    }
  }

  def handle_shutdown () {
    Instrumenter().restart_system
    shutdownSem.acquire
  }

  def handle_start_trace () {
    shutdownSem.release
  }

  def handle_after_receive (cell: ActorCell) : Unit = {
    event_orchestrator.events += ChangeContext("scheduler")
  }

  /**
   * Reset ourselves and the Instrumenter to a initial clean state.
   */
  def reset_state () = {
    println("resetting state...")
    handle_shutdown()
    event_orchestrator = new EventOrchestrator[ExternalEvent]()
    fd = new FDMessageOrchestrator(enqueue_message)
    event_orchestrator.set_failure_detector(fd)
    traceSem = new Semaphore(0)
    currentlyInjecting.set(false)
    shutdownSem = new Semaphore(0)
    messagesToSend = new SynchronizedQueue[(ActorRef, Any)]()
    started.set(false)
    schedSemaphore = new Semaphore(1)
    println("state reset.")
  }
}
