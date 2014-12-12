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

import scala.util.control.Breaks._

// Just a very simple, non-null scheduler that supports
// partitions and injecting external events.
class ReplayScheduler() extends Scheduler {

  var instrumenter = Instrumenter()
  var currentTime = 0
  var index = 0

  // Current set of enabled events.
  val pendingEvents = new HashMap[(String, String, Any), Queue[(ActorCell, Envelope)]]

  val actorNames = new HashSet[String]
  val actorQueue = new Queue[String]
  // Pairs of actors that cannot communicate
  val partitioned = new HashSet[(String, String)]

  // An actor that is unreachable
  val inaccessible = new HashSet[String]

  // Actors to actor ref
  val actorToActorRef = new HashMap[String, ActorRef]
  val actorToSpawnEvent = new HashMap[String, SpawnEvent]

  private[this] var trace: Array[Event] = Array()
  private[this] var traceIdx: Int = 0
  var events: Queue[Event] = new Queue[Event]()

  // Semaphore to wait for trace replay to be done
  private[this] val traceSem = new Semaphore(0)

  // Are we in replay or is someone using this scheduler in some strange way.
  private[this] val replay = new AtomicBoolean(false)

  // Semaphore to use when shutting down the scheduler
  private[this] val shutdownSem = new Semaphore(0)

  private[this] var firstMessage = true

  // A set of messages to send
  val messagesToSend = new HashSet[(ActorRef, Any)]

  // Just do a cheap test to ensure that no new unexpected messages are sent. This
  // is not perfect.
  private[this] val allSends = new HashMap[(String, String, Any), Int]

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
    actor ! msg
    // Wait to make sure all messages are enqueued
    instrumenter.await_enqueue()
  }

  // Given an external event trace, see the events produced
  def replay (_trace: Array[Event],
              _props: Map[String, Props]
              ) : Queue[Event]  = {
    trace = _trace
    events = new Queue[Event]()
    traceIdx = 0
    val actors = HashSet[String]()
    actors += "deadLetters"
    // We begin by starting all actors at the beginning of time, just mark them as
    // isolated (i.e., unreachable)
    for (t <- trace) {
      t match {
        case SpawnEvent (_, props, name, _) =>
          // Just start and isolate all actors we might eventually care about
          instrumenter.actorSystem.actorOf(props, name)
          isolate_node(name)
          actors += name
        case MsgSend (snd, rcv, msg) =>
          allSends((snd, rcv, msg)) = allSends.getOrElse((snd, rcv, msg), 0) + 1
          if (!(actors contains snd)) {
            instrumenter.actorSystem.actorOf(_props(snd), snd)
            isolate_node(snd)
            actors += snd
          }
          if (!(actors contains rcv)) {
            instrumenter.actorSystem.actorOf(_props(rcv), rcv)
            isolate_node(rcv)
            actors += rcv
          }
        case _ =>
          None
      }
    }
    replay.set(true)
    // Start playing back trace
    advanceTrace()
    // Have this thread wait until the trace is down. This allows us to safely notify
    // the caller.
    traceSem.acquire
    replay.set(false)
    return events
  }

  private[this] def advanceTrace() {
    schedSemaphore.acquire
    var loop = true
    breakable {
      while (loop && traceIdx < trace.size) {
        trace(traceIdx) match {
          // TODO(cs): factor this code out. Currently redundant with PeekScheduler's advanceTrace().
          case SpawnEvent (_, _, name, _) =>
            events += actorToSpawnEvent(name)
            unisolate_node(name)
          case KillEvent (name) =>
            events += KillEvent(name)
            isolate_node(name)
          case PartitionEvent(endpoints) =>
            events += PartitionEvent(endpoints)
            add_to_partition(endpoints)
          case UnPartitionEvent(endpoints) =>
            events += UnPartitionEvent(endpoints)
            remove_partition(endpoints)
          case MsgSend (sender, receiver, message) =>
            if (sender == "deadLetters") {
              enqueue_message(receiver, message)
            }
          case MsgEvent(snd, rcv, msg) =>
            break
          case Quiescence =>
            // This is just a nop. Do nothing
            events += Quiescence
          case ChangeContext(_) => () // Check what is going on
        }
        traceIdx += 1
      }
    }
    schedSemaphore.release
    // OK this is the first time round, let us start dispatching
    if (firstMessage) {
      firstMessage = false
      instrumenter.start_dispatch()
    }
  }

  // Check no unexpected messages are enqueued
  override def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    require(allSends((snd, rcv, msg)) > 0)
    allSends((snd, rcv, msg)) = allSends.getOrElse((snd, rcv, msg), 0) - 1
    events += MsgSend(snd, rcv, envelope.message)
    // Drop any messages that crosses a partition.
    if (!((partitioned contains (snd, rcv))
         || (partitioned contains (rcv, snd))
         || (inaccessible contains rcv)
         || (inaccessible contains snd))) {
      val msgs = pendingEvents.getOrElse((snd, rcv, msg),
                          new Queue[(ActorCell, Envelope)])
      pendingEvents((snd, rcv, msg)) = msgs += ((cell, envelope))
    }
  }

  // Record a mapping from actor names to actor refs
  override def event_produced(event: Event) = {
    event match {
      case event : SpawnEvent =>
        if (!(actorNames contains event.name)) {
          actorQueue += event.name
          actorNames += event.name
          actorToActorRef(event.name) = event.actor
        }
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

  // TODO: The first message send ever is not queued, and hence leads to a bug.
  // Solve this someway nice.
  override def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    // First get us to kind of a good place
    advanceTrace
    if (traceIdx >= trace.size) {
      // We are done, let us wait for notify_quiescence to notice this
      // FIXME: We could check here to see if there are any pending messages.
      None
    } else {
      // Ensure that only one thread is accessing shared scheduler structures
      schedSemaphore.acquire

      // Pick next message based on trace. We are guaranteed to be at a MsgEvent
      // unless something else went wrong.
      val key = trace(traceIdx) match {
        case MsgEvent(snd, rcv, msg) =>
          (snd, rcv, msg)
        case _ =>
         throw new Exception("Replay error")
      }
      // Now that we have found the key advance to the next thing to replay
      traceIdx += 1
      val nextMessage = pendingEvents.get(key) match {
        case Some(queue) =>
          if (queue.isEmpty) {
            // Message not enabled
            pendingEvents.remove(key)
            None
          } else {
            val willRet = queue.dequeue()
            if (queue.isEmpty) {
              pendingEvents.remove(key)
            }
            Some(willRet)
          }
        case None =>
          // Message not enabled
          None
      }
      schedSemaphore.release
      nextMessage
    }
  }

  override def notify_quiescence () {
    if (traceIdx < trace.size) {
      // If waiting for quiescence.
      throw new Exception("Divergence")
    } else {
      if (replay.get) {
        println("Done " + pendingEvents)
        // Tell the calling thread we are done
        traceSem.release
      }
    }
  }

  // Get next event
  def next_event() : Event = {
    throw new Exception("NYI")
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

  // Called before we start processing a newly received event
  def before_receive(cell: ActorCell) {
    currentTime += 1
    events += ChangeContext(cell.self.path.name)
  }

  // Called after receive is done being processed
  def after_receive(cell: ActorCell) {
    events += ChangeContext("scheduler")
  }

  // Is this message a system message
  def isSystemCommunication(sender: ActorRef, receiver: ActorRef): Boolean = {
    if (sender == null && receiver == null) return true
    val senderPath = if (sender != null) sender.path.name else "deadLetters"
    val receiverPath = if (receiver != null) receiver.path.name else "deadLetters"
    return isSystemMessage(senderPath, receiverPath)
  }

  def isSystemMessage(src: String, dst: String): Boolean = {
    if ((actorNames contains src) || (actorNames contains dst))
      return false

    return true
  }

}
