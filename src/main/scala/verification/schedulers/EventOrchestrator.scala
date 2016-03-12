package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props, Terminated, ActorRefWithCell}
import akka.actor.ActorDSL._

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

/**
 * Maintains the current state of the network, and provides
 * an interface for triggering events that affect that state, e.g. inducing
 * failures.
 *
 * Additionally records which events occured during the execution, possibly including internal events.
 *
 * Generic E specifies the type of given trace. Usually either Event or
 * ExternalEvent.
 */
class EventOrchestrator[E] {
  // Function that queues a message to be sent later.
  type EnqueueMessage = (Option[ActorRef], String, Any) => Unit

  // Callbacks
  type KillCallback = (String, Set[String], Int) => Unit
  type PartitionCallback = (String, String, Int) => Unit
  type UnPartitionCallback = (String, String, Int) => Unit
  var killCallback : KillCallback = (s: String, actors: Set[String], id: Int) => None
  def setKillCallback(c: KillCallback) = killCallback = c
  var partitionCallback : PartitionCallback = (a: String, b: String, id: Int) => None
  def setPartitionCallback(c: PartitionCallback) = partitionCallback = c
  var unPartitionCallback : UnPartitionCallback = (a: String, b: String, id: Int) => None
  def setUnPartitionCallback(c: UnPartitionCallback) = unPartitionCallback = c

  val log = LoggerFactory.getLogger("EventOrchestrator")

  // Pairs of actors that cannot communicate
  val partitioned = new HashSet[(String, String)]

  // Actors that are unreachable
  val inaccessible = new HashSet[String]

  // Actors that have been explicitly killed.
  // (Same as inaccessble, except that inaccessible also contains actors that
  // have been created but not Start()'ed)
  val killed = new HashSet[String]

  // Actors to actor ref
  // TODO(cs): make getter/setters for these
  val actorToActorRef = new HashMap[String, ActorRef]
  val actorToSpawnEvent = new HashMap[String, SpawnEvent]

  var trace: ArrayBuffer[E] = new ArrayBuffer[E]
  var traceIdx: Int = 0
  var events = new EventTrace

  var fd : FDMessageOrchestrator = null

  def set_trace(_trace: Seq[E]) {
    //println("Setting trace: " + _trace.size)
    trace = new ArrayBuffer() ++ _trace
    traceIdx = 0
  }

  def reset_events() {
    events = new EventTrace(events.original_externals)
  }

  def getRemainingTrace() : Seq[E] = {
    return trace.slice(traceIdx, trace.length)
  }

  def prepend(prefix: Seq[E]) {
    trace.++=:(prefix)
  }

  def trace_advanced() = {
    traceIdx += 1
  }

  def previous_event(): E = {
    if (traceIdx - 0 < 0) {
      throw new IllegalStateException("No previous event..")
    }
    trace(traceIdx - 1)
  }

  def trace_finished() : Boolean = {
    return traceIdx >= trace.size
  }

  def finish_early() = {
    traceIdx = trace.size
  }

  // A bit of a misnomer: current *trace* event, not current recorded event.
  def current_event() : E = {
    if (traceIdx >= trace.length) {
      throw new IllegalStateException("No current event..")
    }
    trace(traceIdx)
  }

  /**
   * Inform the given failure detector of any injected events in the future.
   */
  def set_failure_detector(_fd: FDMessageOrchestrator) = {
    fd = _fd
  }

  /**
   * Injects ExternalEvents in trace until Quiescence it's time for the
   * scheduler to wait for quiescence.
   *
   * Should not be invoked if E != ExternalEvent.
   *
   * Return whether start_dispatch should be invoked after returning.
   */
  def inject_until_quiescence(enqueue_message: EnqueueMessage): Boolean = {
    var loop = true
    while (loop && !trace_finished) {
      log.trace("Injecting " + traceIdx + "/" + trace.length + " " + current_event)
      current_event.asInstanceOf[ExternalEvent] match {
        case Start (_, name) =>
          trigger_start(name)
        case k @ Kill (name) =>
          killCallback(name, actorToActorRef.keys.toSet, k._id)
          trigger_kill(name)
        case k @ HardKill (name) =>
          // If we just delivered a message to the actor we're about to kill,
          // the current thread is still in the process of
          // handling the Mailbox for that actor. In that
          // case we need to wait for the Mailbox to be set to "Idle" before
          // we can kill the actor, since otherwise the Mailbox will not be
          // able to process the akka-internal "Terminated" messages, i.e.
          // killing it now will result in a deadlock.
          if (Instrumenter().previousActor == name) {
            // N.B. because *this* thread is the one that will eventually set
            // the mailbox state to idle, we should be guarenteed that we will
            // not end up with start_dispatch being called concurrently with
            // what we're currently doing here.
            Instrumenter().dispatchAfterMailboxIdle(name)
            return false
          }
          killCallback(name, actorToActorRef.keys.toSet, k._id)
          trigger_hard_kill(k)
        case Send (name, messageCtor) =>
          enqueue_message(None, name, messageCtor())
        case p @ Partition (a, b) =>
          partitionCallback(a,b,p._id)
          trigger_partition(a,b)
        case u @ UnPartition (a, b) =>
          unPartitionCallback(a,b,u._id)
          trigger_unpartition(a,b)
        case WaitCondition(cond) =>
           // Don't let trace advance here. Only let it advance when the
           // condition holds.
          return true
        case c @ CodeBlock(block) =>
          events += c.asInstanceOf[Event] // keep the id the same
          // Since the block might send messages, make sure that we treat the
          // message sends as if they are being triggered by an external
          // thread, i.e. we enqueue them rather than letting them be sent
          // immediately.
          Instrumenter.overrideInternalThreadRule
          // This better terminate!
          block()
          Instrumenter.unsetInternalThreadRuleOverride
        case WaitQuiescence() =>
          events += BeginWaitQuiescence
          loop = false // Start waiting for quiescence
      }
      trace_advanced()
    }
    return true
  }

  // Mark a couple of nodes as partitioned (so they cannot communicate)
  def add_to_partition (newly_partitioned: (String, String)) {
    partitioned += newly_partitioned
  }

  // Mark a couple of node as unpartitioned, i.e. they can communicate
  def remove_partition (newly_partitioned: (String, String)) {
    partitioned -= newly_partitioned
  }

  // Mark a node as unreachable, used to kill a node.
  // TODO(cs): to be implemented later: actually kill the node so that its state is cleared?
  def isolate_node (node: String) {
    inaccessible += node
    if (fd != null) {
      fd.isolate_node(node)
    }
  }

  // Mark a node as reachable, also used to start a node
  def unisolate_node (actor: String) {
    inaccessible -= actor
    killed -= actor
    if (fd != null) {
      fd.unisolate_node(actor)
    }
  }

  def trigger_start (name: String) = {
    events += actorToSpawnEvent(name)
    Util.logger.log(name, "God spawned me")
    unisolate_node(name)
    // If actor was previously killed, allow scheduler to send messages to it
    // again.
    if (Instrumenter().blockedActors contains name) {
      Instrumenter().blockedActors = Instrumenter().blockedActors - name
    }
    if (fd != null) {
      fd.handle_start_event(name)
    }
  }

  def trigger_kill (name: String) = {
    events += KillEvent(name)
    Util.logger.log(name, "God killed me")
    killed += name
    isolate_node(name)
    if (fd != null) {
      fd.handle_kill_event(name)
    }
  }

  def trigger_hard_kill (hardKill: HardKill) = {
    events += hardKill
    val name = hardKill.name
    Util.logger.log(name, "God is about to hard kill me")
    Instrumenter().preStartCalled.synchronized {
      // Actor should already have been fully initialized
      // TODO(cs): should probably synchronize over this whole method..
      assert(Instrumenter().actorMappings contains name)
      val ref = Instrumenter().actorMappings(name)
      assert(!(Instrumenter().preStartCalled contains ref))
    }

    // Get all the information we need before killing
    // TODO(cs): this may need to be recursive.
    val children = actorToActorRef(name).asInstanceOf[ActorRefWithCell].underlying.
      childrenRefs.children.map(ref => ref.path.name).toSeq
    val ref = actorToActorRef(name)

    // Clean up our data before killing
    log.debug("Cleaning up before hard kill...")
    for (name <- Seq(name) ++ children) {
      actorToActorRef -= name
      actorToSpawnEvent -= name
      // TODO(cs): move this into Instrumenter
      Instrumenter().actorMappings.synchronized {
        Instrumenter().dispatchers -= Instrumenter().actorMappings(name)
        Instrumenter().actorMappings -= name
      }
      // Unfortunately, all of the scheduler's pointers to this ActorCell may now become invalid,
      // i.e. the ActorCell's fields may now change dynamically and point to
      // another actor or be nulled out. See:
      // https://github.com/akka/akka/blob/release-2.2/akka-actor/src/main/scala/akka/actor/ActorCell.scala#L613
      // So we have scheduler remove all references to the actor cells.
      // TODO(cs): resend the pending messages for this actor, i.e. simulate a situation where
      // packets in the network destined for the old actor arrive at the newly
      // restarted actor. Tricky to deal with depGraph..
      val sndMsgPairs = Instrumenter().scheduler.actorTerminated(name)
      Instrumenter().blockedActors = Instrumenter().blockedActors - name
      Instrumenter().timerToCancellable.keys.foreach {
        case (rcv,msg) =>
          if (rcv == name) {
            val cancellable = Instrumenter().timerToCancellable((rcv,msg))
            Instrumenter().removeCancellable(cancellable)
          }
      }
    }

    // stop()'ing is asynchronous. So, block until it completes.
    implicit val system = Instrumenter()._actorSystem
    val i = inbox()
    i watch ref
    // Kill it
    log.debug("Calling stop() and blocking... " + ref)
    Instrumenter()._actorSystem.stop(ref)
    // Now block
    def isTerminated(msg: Any) : Boolean = {
      msg match {
        case Terminated(_) => true
        case _ => false
      }
    }
    var msg = i.receive()
    while (!isTerminated(msg)) {
      msg = i.receive()
    }

    if (fd != null) {
      fd.handle_kill_event(name)
    }
  }

  def trigger_partition (a: String, b: String) = {
    events += PartitionEvent((a, b))
    add_to_partition((a, b))
    Util.logger.log(a, "God partitioned me from " + b)
    Util.logger.log(b, "God partitioned me from " + a)
    if (fd != null) {
      fd.handle_partition_event(a,b)
    }
  }

  def trigger_unpartition (a: String, b: String) = {
    events += UnPartitionEvent((a, b))
    remove_partition((a, b))
    Util.logger.log(a, "God reconnected me to " + b)
    Util.logger.log(b, "God reconnected me to " + a)
    if (fd != null) {
      fd.handle_unpartition_event(a,b)
    }
  }

  def handle_spawn_produced (event: SpawnEvent) = {
    actorToActorRef(event.name) = event.actor
    if (fd != null) {
      fd.create_node(event.name)
    }
  }

  def handle_spawn_consumed (event: SpawnEvent) = {
    actorToSpawnEvent(event.name) = event
  }

  def crosses_partition (snd: String, rcv: String) : Boolean = {
    if (snd == rcv && !(killed contains snd)) return false
    return ((partitioned contains (snd, rcv))
           || (partitioned contains (rcv, snd))
           || (inaccessible contains rcv)
           || (inaccessible contains snd))
  }
}
