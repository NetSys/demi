package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet
import scala.collection.mutable.Iterable
import scala.collection.generic.Clearable

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

abstract class MessageType()
final case object ExternalMessage extends MessageType
final case object InternalMessage extends MessageType
final case object SystemMessage extends MessageType
final case object CheckpointMessage extends MessageType

/**
 * A mix-in for schedulers that take external events as input, and generate
 * executions containing both external and internal events as output.
 */
trait ExternalEventInjector[E] {

  var event_orchestrator = new EventOrchestrator[E]()

  // Handler for FailureDetector messages
  var fd : FDMessageOrchestrator = new FDMessageOrchestrator(enqueue_message)
  event_orchestrator.set_failure_detector(fd)
  var _disableFailureDetector = false

  def disableFailureDetector() {
    _disableFailureDetector = true
    event_orchestrator.set_failure_detector(null)
  }

  // Handler for Checkpoint responses
  var checkpointer : CheckpointCollector = null
  var _enableCheckpointing = false
  def enableCheckpointing() {
    _enableCheckpointing = true
    checkpointer = new CheckpointCollector
  }

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

  // Semaphore to use when taking a checkpooint. We initialize the
  // semaphore to 0 rather than 1, so that the main thread blocks upon
  // invoking acquire() until another thread release()'s it.
  var checkpointSem = new Semaphore(0)

  // Whether we are currently collecting a checkpoint from the application.
  var currentlyCheckpointing = new AtomicBoolean(false)

  // Are we expecting message receives
  val started = new AtomicBoolean(false)

  // Ensure that only one thread is running inside the scheduler when we are
  // dispatching external messages to actors. (Does not guard the scheduler's instance
  // variables.)
  var schedSemaphore = new Semaphore(1)

  // If we enqueued an external message, keep track of it, so that we can
  // later identify it as an external message when it is plumbed through
  // event_produced
  // Assumes that external message objects never == internal message objects.
  // That assumption would be broken if, for example, nodes relayed external
  // messages to eachother...
  var enqueuedExternalMessages = new MultiSet[Any]

  // A set of external messages to send. Messages sent between actors are not
  // queued here.
  var messagesToSend = new SynchronizedQueue[(ActorRef, Any)]()

  // Whenever we inject a "Continue" external event, this tracks how many
  // events we have left to scheduled until we return to scheduling more
  // external events.
  var numWaitingFor = new AtomicInteger(0)

  // Enqueue an external message for future delivery
  def enqueue_message(receiver: String, msg: Any) {
    if (event_orchestrator.actorToActorRef contains receiver) {
      enqueue_message(event_orchestrator.actorToActorRef(receiver), msg)
    } else {
      throw new IllegalArgumentException("Unknown receiver " + receiver)
    }
  }

  def enqueue_message(actor: ActorRef, msg: Any) {
    enqueuedExternalMessages += msg
    messagesToSend += ((actor, msg))
  }

  def send_external_messages() {
    send_external_messages(true)
  }

  // Initiates message sends for all messages in messagesToSend. Note that
  // delivery does not occur immediately! These messages will subsequently show
  // up in event_produced as messages to be scheduled by schedule_new_message.
  def send_external_messages(acquireSemaphore: Boolean) {
    // Ensure that only one thread is accessing shared scheduler structures
    if (acquireSemaphore) {
      schedSemaphore.acquire
    }
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
    if (acquireSemaphore) {
      schedSemaphore.release
    }
  }

  // Given an external event trace, see the events produced
  def execute_trace (_trace: Seq[E]) : EventTrace = {
    event_orchestrator.set_trace(_trace)
    event_orchestrator.reset_events
    // TODO(cs): assumes that we never actually kill actors, only
    // (permanently) isolate them.
    val uniqueActors = new HashSet[String]
    // We begin by starting all actors at the beginning of time, just mark them as
    // isolated (i.e., unreachable). Later, when we replay the `Start` event,
    // we unisolate the actor.
    for (t <- event_orchestrator.trace) {
      t match {
        case Start (propCtor, name) =>
          // Just start and isolate all actors we might eventually care about [top-level actors]
          Instrumenter().actorSystem.actorOf(propCtor(), name)
          event_orchestrator.isolate_node(name)
          uniqueActors += name
        case _ =>
          None
      }
    }
    if (!_disableFailureDetector) {
      fd.startFD(Instrumenter().actorSystem)
    }
    if (_enableCheckpointing) {
      checkpointer.startCheckpointCollector(Instrumenter().actorSystem,
                                            uniqueActors.size)
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
    if (!event_orchestrator.trace_finished) {
      event_orchestrator.current_event match {
        case Continue(n) => numWaitingFor.set(n)
        case _ => None
      }
    }
    schedSemaphore.release
    // Since this is always called during quiescence, once we have processed all
    // events, let us start dispatching
    Instrumenter().start_dispatch()
  }

  def takeCheckpoint() : HashMap[String, CheckpointReply] = {
    println("Initiating checkpoint")
    if (!_enableCheckpointing) {
      throw new IllegalStateException("Must invoke enableCheckpointing() first")
    }
    currentlyCheckpointing.set(true)
    checkpointer.checkpoints.clear()
    val actors = event_orchestrator.actorToActorRef.keys.
                                    filterNot(ActorTypes.systemActor)
    // Put our requests at the front of the queue, and any existing requests
    // at the end of the queue.
    val existingExternals = new Queue[(ActorRef, Any)] ++ messagesToSend
    messagesToSend.clear
    for (actor <- actors) {
      enqueue_message(actor, CheckpointRequest)
    }
    messagesToSend ++= existingExternals
    val wasQuiescent = !started.get
    if (wasQuiescent) {
      started.set(true)
      Instrumenter().start_dispatch()
    }
    checkpointSem.acquire()
    if (wasQuiescent) {
      // Block again! We got all of our checkpoint replies, but we
      // need to wait for quiescence to return ourselves to
      // the original state. This time, notify_quiescence will call release()
      checkpointSem.acquire()
      started.set(false)
    }
    currentlyCheckpointing.set(false)
    return checkpointer.checkpoints
  }

  /**
   * Return `Internal` object if the event is an internal event,
   * `External` if the event is an externally triggered messages destined
   * towards an actor, else `System` if it is not destined towards an actor.
   */
  def handle_event_produced(snd: String, rcv: String, envelope: Envelope) : MessageType = {
    // Intercept any messages sent towards the failure detector
    if (rcv == FailureDetector.fdName) {
      if (!_disableFailureDetector) {
        fd.handle_fd_message(envelope.message, snd)
      }
      return SystemMessage
    } else if (rcv == CheckpointSink.name) {
      if (_enableCheckpointing) {
        checkpointer.handleCheckpointResponse(envelope.message, snd)
        if (checkpointer.done) {
          checkpointSem.release()
        }
      }
      return CheckpointMessage
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
    numWaitingFor.decrementAndGet()
    val rcv = cell.self.path.name
    val msg = envelope.message
    if (enqueuedExternalMessages.contains(msg)) {
      enqueuedExternalMessages -= msg
    }
    assert(started.get)
    event_orchestrator.events += ChangeContext(rcv)
  }

  def handle_quiescence () {
    assert(started.get)
    started.set(false)
    event_orchestrator.events += Quiescence
    if (numWaitingFor.get() > 0) {
      Instrumenter().await_timers(1)
      started.set(true)
      Instrumenter().start_dispatch()
    } else if (currentlyCheckpointing.get()) {
      checkpointSem.release()
    } else if (!event_orchestrator.trace_finished) {
      // If waiting for quiescence.
      advanceTrace()
    } else {
      if (currentlyInjecting.get) {
        // Tell the calling thread we are done
        traceSem.release
      } else {
        throw new RuntimeException("currentlyInjecting.get returned false")
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

  def handle_before_receive (cell: ActorCell) : Unit = {
    event_orchestrator.events += ChangeContext(cell.self.path.name)
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
    event_orchestrator = new EventOrchestrator[E]()
    fd = new FDMessageOrchestrator(enqueue_message)
    if (!_disableFailureDetector) {
      event_orchestrator.set_failure_detector(fd)
    }
    if (_enableCheckpointing) {
      checkpointer = new CheckpointCollector
    }
    currentlyCheckpointing = new AtomicBoolean(false)
    checkpointSem = new Semaphore(0)
    traceSem = new Semaphore(0)
    currentlyInjecting.set(false)
    shutdownSem = new Semaphore(0)
    started.set(false)
    schedSemaphore = new Semaphore(1)
    enqueuedExternalMessages = new MultiSet[Any]
    messagesToSend = new SynchronizedQueue[(ActorRef, Any)]
    println("state reset.")
  }
}
