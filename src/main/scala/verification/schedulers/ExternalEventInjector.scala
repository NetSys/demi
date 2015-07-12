package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, Cell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet
import scala.collection.mutable.SynchronizedSet
import scala.collection.mutable.Iterable
import scala.collection.generic.Clearable

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

abstract class MessageType()
final case object ExternalMessage extends MessageType
final case object InternalMessage extends MessageType
final case object FailureDetectorQuery extends MessageType
final case object CheckpointReplyMessage extends MessageType

/**
  * A mix-in for schedulers that take external events as input, and generate
  * executions containing both external and internal events as output.
  */
trait ExternalEventInjector[E] {
  // Must be defined in inheritors:
  val schedulerConfig: SchedulerConfig

  var event_orchestrator = new EventOrchestrator[E]()

  // Handler for FailureDetector messages
  var fd : FDMessageOrchestrator = null
  if (schedulerConfig.enableFailureDetector) {
    fd = new FDMessageOrchestrator(enqueue_message)
    event_orchestrator.set_failure_detector(fd)
  } else {
    event_orchestrator.set_failure_detector(null)
  }

  // Handler for Checkpoint responses
  var checkpointer : CheckpointCollector = null
  if (schedulerConfig.enableCheckpointing) {
    checkpointer = new CheckpointCollector
  }

  // A list of all *possible* actors, not just all live actors.
  var actorNamePropPairs: Seq[Tuple2[Props,String]] = null
  // If invoked, will always populate these actors, not just those with Start
  // events.
  def setActorNamePropPairs(_actorNamePropPairs: Seq[Tuple2[Props,String]]) {
    actorNamePropPairs = _actorNamePropPairs
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

  // Semaphore to use when taking a checkpoint. We initialize the
  // semaphore to 0 rather than 1, so that the main thread blocks upon
  // invoking acquire() until another thread release()'s it.
  var checkpointSem = new Semaphore(0)

  // Whether a thread is blocked waiting for a checkpoint.
  var blockedOnCheckpoint = new AtomicBoolean(false)

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
  // TODO(cs): include receiver here for safety?
  var enqueuedExternalMessages = new MultiSet[Any]

  // A set of external messages to send. Messages sent between actors are not
  // queued here. Tuple is: (sender, receiver, msg)
  var messagesToSend = new SynchronizedQueue[(Option[ActorRef], ActorRef, Any)]()

  // Whether populateActors has been invoked.
  var alreadyPopulated = false

  // In case we're in non-blocking mode.
  var terminationCallback : Option[(EventTrace) => Any] = None

  // If the last event we replayed is WaitCondition, and we've reached
  // quiescence, signal that once we've waited for enqueue_message() to be
  // invoked we should start dispatch again.
  var dispatchAfterEnqueueMessage = new AtomicBoolean(false)

  // An optional callback that will be invoked when a WaitQuiescence has just
  // caused us to arrive at Quiescence.
  type QuiescenceCallback = () => Unit
  var quiescenceCallback : QuiescenceCallback = () => None
  def setQuiescenceCallback(c: QuiescenceCallback) { quiescenceCallback = c }

  // Whether we are currently processing an "UnignorableEvents" block
  var unignorableEvents = new AtomicBoolean(false)

  // All external thread ids that recently began an "AtomicBlock"
  var beganExternalAtomicBlocks = new MultiSet[Long] with SynchronizedSet[Long]

  // All external thread ids that recently ended an "AtomicBlock"
  var endedExternalAtomicBlocks = new MultiSet[Long] with SynchronizedSet[Long]

  // how many external atomic blocks are currently running
  var pendingExternalAtomicBlocks = new AtomicInteger(0)

  /**
   * Until endUnignorableEvents is invoked, mark all events that we record
   * as "unignorable", i.e., during replay, don't ever skip over them.
   *
   * Called by external threads.
   */
  def beginUnignorableEvents() {
    assert(!unignorableEvents.get())
    unignorableEvents.set(true)
    // TODO(cs): should these be placed into messagesToSend too?
    event_orchestrator.events += BeginUnignorableEvents
  }

  /**
   * Pre: beginUnignorableEvents was previously invoked.
   *
   * Called by external threads.
   */
  def endUnignorableEvents() {
    assert(unignorableEvents.get())
    println("endUnignorableEvents")
    unignorableEvents.set(false)
    // TODO(cs): should these be placed into messagesToSend too?
    event_orchestrator.events += EndUnignorableEvents
  }

  /**
   * Within receive(), an internal thread has just created an external thread.
   * (before that external thread has necessarily been scheduled).
   *
   * This method indicates the start of the external thread's `atomic block`, where
   * it will now send some number of messages. Upon replay, wait until the end of the
   * atomic block before deciding whether those messages are or are not going
   * to show up.
   */
  def beginExternalAtomicBlock(taskId: Long) {
    // Place the marker into the current place in messagesToSend; any messages
    // already enqueued before this are not part of the
    // beginExternalAtomicBlock
    beganExternalAtomicBlocks.synchronized {
      beganExternalAtomicBlocks += taskId
    }
    messagesToSend += ((None, null, BeginExternalAtomicBlock(taskId)))
    pendingExternalAtomicBlocks.incrementAndGet()
    // We shouldn't be dispatching while the atomic block executes.
    Instrumenter().stopDispatch.set(true)
  }

  /**
   * Pre: beginExternalAtomicBlock was previously invoked.
   *
   * Called by external threads.
   */
  def endExternalAtomicBlock(taskId: Long) {
    // Place the marker into the current place in messagesToSend; any messages
    // already enqueued before this are part of the atomic block.
    messagesToSend += ((None, null, EndExternalAtomicBlock(taskId)))
    // Signal that the main thread should invoke send_external_messages
    endedExternalAtomicBlocks.synchronized {
      endedExternalAtomicBlocks.notifyAll()
    }
    if (pendingExternalAtomicBlocks.decrementAndGet() == 0) {
      var restartDispatch = false
      Instrumenter().stopDispatch.synchronized {
        if (Instrumenter().stopDispatch.get()) {
          // Still haven't stopped dispatch, so don't restart
          Instrumenter().stopDispatch.set(false)
        } else {
          restartDispatch = true
        }
      }
      if (restartDispatch) {
        println("Restarting dispatch!")
        Instrumenter().start_dispatch
      }
    }
  }

  // Enqueue an external message for future delivery
  def enqueue_message(sender: Option[ActorRef], receiver: String, msg: Any) {
    if (event_orchestrator.actorToActorRef contains receiver) {
      enqueue_message(sender, event_orchestrator.actorToActorRef(receiver), msg)
    } else {
      println("WARNING! Unknown message receiver " + receiver)
    }
  }

  def enqueue_message(sender: Option[ActorRef], actor: ActorRef, msg: Any) {
    enqueuedExternalMessages += msg

    // signal to the main thread that it should wake up if it's blocked on
    // external messages
    messagesToSend.synchronized {
      messagesToSend += ((sender, actor, msg))
      messagesToSend.notifyAll()
    }

    dispatchAfterEnqueueMessage.synchronized {
      if (dispatchAfterEnqueueMessage.get()) {
        dispatchAfterEnqueueMessage.set(false)
        println("dispatching after enqueue_message")
        started.set(true)
        send_external_messages()
        Instrumenter().start_dispatch()
      }
    }
  }

  // Enqueue a timer message for future delivery
  def handle_timer(receiver: String, msg: Any): Unit = {
    if (schedulerConfig.ignoreTimers) {
      return
    }

    if (event_orchestrator.actorToActorRef contains receiver) {
      // signal to the main thread that it should wake up if it's blocked on
      // external messages
      messagesToSend.synchronized {
        messagesToSend += ((None, event_orchestrator.actorToActorRef(receiver), msg))
        messagesToSend.notifyAll()
      }

    } else {
      println("WARNING! Unknown timer receiver " + receiver)
    }
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
    if (fd != null) {
      fd.send_all_pending_responses()
    }
    // Drain message queue
    Instrumenter().sendKnownExternalMessages(() => {
      messagesToSend.synchronized {
        for ((senderOpt, receiver, msg) <- messagesToSend) {
          // Check if the message is actually a special marker
          msg match {
            case BeginExternalAtomicBlock(taskId) =>
              event_orchestrator.events += BeginExternalAtomicBlock(taskId)
            case EndExternalAtomicBlock(taskId) =>
              endedExternalAtomicBlocks.synchronized {
                endedExternalAtomicBlocks += taskId
                endedExternalAtomicBlocks.notifyAll()
              }
              event_orchestrator.events += EndExternalAtomicBlock(taskId)
            case _ =>
              // It's a normal message
              if (!Instrumenter().receiverIsAlive(receiver)) {
                println("Dropping message to non-existent receiver: " +
                        receiver + " " + msg)
              } else {
                senderOpt match {
                  case Some(sendRef) =>
                    receiver.!(msg)(sendRef)
                  case None =>
                    receiver ! msg
                }
              }
          }
        }
        messagesToSend.clear()
      }
    })

    // Wait to make sure all messages are enqueued
    Instrumenter().await_enqueue()

    // schedule_new_message is reenterant, hence release before calling.
    if (acquireSemaphore) {
      schedSemaphore.release
    }
  }

  // When deserializing an event trace, we need the actors to be prepopulated
  // so we can resolve serialized ActorRefs. Here we populate the actor system
  // give the names and props of all actors that will eventually appear in the
  // execution.
  def populateActorSystem(_actorNamePropPairs: Seq[Tuple2[Props,String]]) = {
    alreadyPopulated = true
    for ((props, name) <- _actorNamePropPairs) {
      // Just start and isolate all actors we might eventually care about
      Instrumenter().actorSystem.actorOf(props, name)
      event_orchestrator.isolate_node(name)
    }
  }

  // Given an external event trace, see the events produced
  // if terminationCallback != None, don't block the main thread!
  def execute_trace (_trace: Seq[E with ExternalEvent],
                     _terminationCallback: Option[(EventTrace) => Any]=None)
                   : EventTrace = {
    terminationCallback = _terminationCallback
    MessageTypes.sanityCheckTrace(_trace)
    event_orchestrator.set_trace(_trace)
    event_orchestrator.reset_events

    if (schedulerConfig.enableFailureDetector) {
      fd.startFD(Instrumenter().actorSystem)
    }
    if (schedulerConfig.enableCheckpointing) {
      checkpointer.startCheckpointCollector(Instrumenter().actorSystem)
    }

    if (!alreadyPopulated) {
      if (actorNamePropPairs != null) {
        populateActorSystem(actorNamePropPairs)
      } else {
        populateActorSystem(_trace flatMap {
          case Start(propCtor,name) => Some((propCtor(), name))
          case _ => None
        })
      }
    }

    currentlyInjecting.set(true)
    // Start playing back trace
    advanceTrace()
    terminationCallback match {
      case None =>
        // Have this thread wait until the trace is down. This allows us to safely notify
        // the caller.
        traceSem.acquire
        currentlyInjecting.set(false)
        return event_orchestrator.events
      case Some(f) =>
        // Don't block. We'll call terminationCallback when we're done.
        return null
    }
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
   * It is the responsibility of the caller to schedule CheckpointRequest
   * messages in schedule_new_message() and return to quiescence once all
   * CheckpointRequests have been sent.
   *
   * N.B. this method blocks. The system must be quiescent before
   * you invoke this. (See RandomScheduler.scala for an example of how to take
   * checkpoints mid-execution without blocking.)
   */
  def takeCheckpoint() : HashMap[String, Option[CheckpointReply]] = {
    println("Initiating checkpoint")
    if (!schedulerConfig.enableCheckpointing) {
      throw new IllegalStateException("Trying to take checkpoint, yet !schedulerConfig.enableCheckpointing")
    }
    blockedOnCheckpoint.set(true)
    prepareCheckpoint()
    require(!started.get)
    started.set(true)
    Instrumenter().start_dispatch()
    checkpointSem.acquire()
    blockedOnCheckpoint.set(false)
    return checkpointer.checkpoints
  }

  def prepareCheckpoint() = {
    if (!schedulerConfig.enableCheckpointing) {
      throw new IllegalStateException("Trying to take checkpoint, yet !schedulerConfig.enableCheckpointing")
    }
    val actorRefs = event_orchestrator.actorToActorRef.
                      filterNot({case (k,v) => ActorTypes.systemActor(k)}).
                      values.toSeq
    val checkpointRequests = checkpointer.prepareRequests(actorRefs)
    // Put our requests at the front of the queue, and any existing requests
    // at the end of the queue.
    val existingExternals = new Queue[(Option[ActorRef], ActorRef, Any)] ++ messagesToSend
    messagesToSend.clear
    for ((actor, request) <- checkpointRequests) {
      enqueue_message(None, actor, request)
    }
    messagesToSend ++= existingExternals
  }

  /**
   * Return `Internal` object if the event is an internal event,
   * `External` if the event is an externally triggered messages destined
   * towards an actor, else `System` if it is not destined towards an actor.
   */
  def handle_event_produced(snd: String, rcv: String, envelope: Envelope) : MessageType = {
    // Intercept any messages sent towards the failure detector
    if (rcv == FailureDetector.fdName) {
      if (schedulerConfig.enableFailureDetector) {
        fd.handle_fd_message(envelope.message, snd)
      }
      return FailureDetectorQuery
    } else if (rcv == CheckpointSink.name) {
      if (schedulerConfig.enableCheckpointing) {
        checkpointer.handleCheckpointResponse(envelope.message, snd)
      }
      return CheckpointReplyMessage
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

  def handle_event_consumed(cell: Cell, envelope: Envelope) = {
    val rcv = cell.self.path.name
    val msg = envelope.message
    if (enqueuedExternalMessages.contains(msg)) {
      enqueuedExternalMessages -= msg
    }
    assert(started.get)
    event_orchestrator.events += ChangeContext(rcv)
  }

  def handle_quiescence(): Unit = {
    assert(started.get)
    started.set(false)
    if (blockedOnCheckpoint.get()) {
      checkpointSem.release()
      return
    }

    if (!event_orchestrator.trace_finished) {
      // If waiting for quiescence.
      event_orchestrator.events += Quiescence
      event_orchestrator.current_event match {
        case WaitCondition(cond) =>
          if (cond()) {
            event_orchestrator.trace_advanced()
            advanceTrace()
          } else {
            // wait for enqueue_message to be invoked.
            println("waiting for enqueue_message...")
            dispatchAfterEnqueueMessage.synchronized {
              dispatchAfterEnqueueMessage.set(true)
            }
          }
        case _ =>
          quiescenceCallback()
          advanceTrace()
      }
    } else {
      quiescenceCallback()
      if (currentlyInjecting.get) {
        // Tell the calling thread we are done
        terminationCallback match {
          case None => traceSem.release
          case Some(f) => f(event_orchestrator.events)
        }
      } else {
        throw new RuntimeException("currentlyInjecting.get returned false")
      }
    }
  }

  def handle_shutdown () {
    alreadyPopulated = false
    Instrumenter().restart_system
    shutdownSem.acquire
  }

  def handle_start_trace () {
    shutdownSem.release
  }

  def handle_before_receive (cell: Cell) : Unit = {
    event_orchestrator.events += ChangeContext(cell.self.path.name)
  }

  def handle_after_receive (cell: Cell) : Unit = {
    event_orchestrator.events += ChangeContext("scheduler")
  }

  // Return true if we found the timer in our messagesToSend
  def handle_timer_cancel(rcv: ActorRef, msg: Any): Boolean = {
    messagesToSend.dequeueFirst(tuple =>
      tuple._2 != null &&
      tuple._2.path.name == rcv.path.name && tuple._3 == msg) match {
      case Some(_) => return true
      case None => return false
    }
  }

  /**
   * Reset ourselves and the Instrumenter to a initial clean state.
   */
  def reset_state (shutdown: Boolean) = {
    println("resetting state...")
    if (shutdown) {
      handle_shutdown()
    }
    event_orchestrator = new EventOrchestrator[E]()
    fd = new FDMessageOrchestrator(enqueue_message)
    if (schedulerConfig.enableFailureDetector) {
      event_orchestrator.set_failure_detector(fd)
    }
    if (schedulerConfig.enableCheckpointing) {
      checkpointer = new CheckpointCollector
    }
    blockedOnCheckpoint = new AtomicBoolean(false)
    checkpointSem = new Semaphore(0)
    traceSem = new Semaphore(0)
    currentlyInjecting.set(false)
    shutdownSem = new Semaphore(0)
    started.set(false)
    schedSemaphore = new Semaphore(1)
    enqueuedExternalMessages = new MultiSet[Any]
    unignorableEvents = new AtomicBoolean(false)
    beganExternalAtomicBlocks = new MultiSet[Long] with SynchronizedSet[Long]
    endedExternalAtomicBlocks = new MultiSet[Long] with SynchronizedSet[Long]
    pendingExternalAtomicBlocks = new AtomicInteger(0)
    dispatchAfterEnqueueMessage = new AtomicBoolean(false)
    messagesToSend = new SynchronizedQueue[(Option[ActorRef], ActorRef, Any)]
    alreadyPopulated = false
    println("state reset.")
  }
}
