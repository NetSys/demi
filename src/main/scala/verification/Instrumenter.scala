package akka.dispatch.verification

import akka.actor.ActorCell
import akka.actor.Cell
import akka.actor.ActorSystem
import akka.actor.ActorSystemImpl
import akka.actor.ActorRef
import akka.actor.ActorPath
import akka.pattern.PromiseActorRef
import akka.actor.Actor
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Cancellable
import akka.actor.RootActorPath
import akka.actor.Address
import akka.actor.Nobody
import akka.dispatch.Mailbox
import akka.cluster.VectorClock
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.Semaphore
import java.io.Closeable

import com.typesafe.config.Config

import akka.dispatch.Envelope
import akka.dispatch.MessageQueue
import akka.dispatch.MessageDispatcher

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Queue
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import scala.concurrent.Future

import scala.util.Random
import scala.util.control.Breaks

import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger


// Wrap cancellable so we get notified about cancellation
class WrappedCancellable (c: Cancellable, rcv: String, msg: Any) extends Cancellable {
  val instrumenter = Instrumenter()
  def cancel(): Boolean = {
    val success = c.cancel
    instrumenter.cancelTimer(this, rcv, msg, success)
    return success
  }

  def isCancelled: Boolean = c.isCancelled
}

class InstrumenterCheckpoint(
  val actorMappings : HashMap[String, ActorRef],
  val seenActors : HashSet[(ActorSystem, Any)],
  val allowedEvents: HashSet[(ActorCell, Envelope)],
  val dispatchers : HashMap[ActorRef, MessageDispatcher],
  val vectorClocks : HashMap[String, VectorClock],
  val sys: ActorSystem,
  var cancellableToTimer : HashMap[Cancellable, Tuple2[String, Any]],
  var ongoingCancellableTasks : HashSet[Cancellable],
  var timerToCancellable : HashMap[Tuple2[String,Any], Cancellable],
  // TODO(cs): add the random associated with this actor system
  val applicationCheckpoint: Any
) {}

class Instrumenter {
  // Provides the application a hook to compute after each shutdown
  type ShutdownCallback = () => Unit
  // Checkpoints the application state (outside the actor system)
  type CheckpointCallback = () => Any
  // Restores the application state.
  type RestoreCheckpointCallback = (Any) => Unit

  var scheduler : Scheduler = new NullScheduler
  var tellEnqueue : TellEnqueue = new TellEnqueueSemaphore
  
  val dispatchers = new HashMap[ActorRef, MessageDispatcher]
  
  val allowedEvents = new HashSet[(ActorCell, Envelope)]  
  
  val seenActors = new HashSet[(ActorSystem, Any)]
  // Populated before preStart has been called
  val actorMappings = new HashMap[String, ActorRef]
  // Populated after preStart has been called
  val preStartCalled = new HashSet[ActorRef]
  
  // Track the executing context (i.e., source of events)
  var currentActor = ""
  var previousActor = ""
  var inActor = new AtomicBoolean(false)
  var counter = 0   
  // Whether we're currently dispatching.
  var started = new AtomicBoolean(false)
  // Whether to ignore (pass-through) all events
  var _passThrough = new AtomicBoolean(false)
  // Whether to stop dispatch at the next afterMessageReceive()
  var stopDispatch = new AtomicBoolean(false)
  // If an actor blocks while it is `receive`ing, we need to continue making
  // progress by finding a new message to schedule. While that actor is
  // blocked, we should not deliver any messages to it. This data structure
  // tracks which actors are currently blocked.
  // For a detailed design doc see:
  // https://docs.google.com/document/d/1RnCDOQFLa2prliF5y5VDNcdGDmEeOlgUcXSNk7abpSo
  // { parent -> value of currentPendingDispatch at the time the parent became blocked }
  var blockedActors = Map[String,(String,Any)]()
  // Mapping from temp actors (used for `ask`) to the name of the actor that created them.
  // For a detailed design doc see:
  // https://docs.google.com/document/d/1_LUceHvQoamlBtNNbqA4CxBH-zvUKlZhnSjLwTc16q4
  var tempToParent = new HashMap[ActorPath, String]
  // After an answer has been sent but before it has been scheduled for
  // delivery, we store the temp actor (receipient) here to mark it as
  // pending.
  var askAnswerNotYetScheduled = new HashSet[PromiseActorRef]
  // If the parent (asker) of a temp actor was *not* blocked on the `ask`,
  // make sure that the scheduling loop keeps going after the ask answer is
  // delivered.
  var dispatchAfterAskAnswer = new AtomicBoolean(false)
  // Set to true an when external thread signals that it wants to send
  // messages in a thread-safe manner (rather than having its sends delayed)
  var sendingKnownExternalMessages = new AtomicBoolean(false)
  var shutdownCallback : ShutdownCallback = () => {}
  var checkpointCallback : CheckpointCallback = () => {null}
  var restoreCheckpointCallback : RestoreCheckpointCallback = (a: Any) => {}
  var registeredCancellableTasks = new HashSet[Cancellable]
  // Mark which of the timers are scheduleOnce vs schedule [ongoing]
  var ongoingCancellableTasks = new HashSet[Cancellable]
  // Track which cancellables correspond to which messages
  var cancellableToTimer = new HashMap[Cancellable, Tuple2[String, Any]]
  // And vice versa
  var timerToCancellable = new HashMap[Tuple2[String,Any], Cancellable]
  // Fake actor, used to designate a code block (as opposed to a message) scheduled through
  // akka.scheduler. Should never receive messages; only used for its ActorRef.
  var scheduleFunctionRef : ActorRef = null
  // Messages sent within a scheduled code block.
  var codeBlockSends = new MultiSet[(String,Any)]
  // Which threads have been spawned to execute scheduled code blocks
  val codeBlockThreads = new HashSet[Thread]
  // For checking asserts
  var currentPendingDispatch = new AtomicReference[Option[(String,Any)]](None)
  // See: SupervisorStrategy.scala
  var crashedActors = new HashSet[String]

  val logger = LoggerFactory.getLogger("Instrumenter")

  private[dispatch] def cancelTimer (c: Cancellable, rcv: String, msg: Any, success: Boolean) = {
    // Need this here since by the time DPORwHeuristics gets here the thing is already canceled
    if (cancellableToTimer contains c) {
      removeCancellable(c)
    }
    if (scheduler != null) {
      scheduler.notify_timer_cancel(rcv, msg)
    }
  }

  def defaultAkkaConfig : com.typesafe.config.Config = {
    ConfigFactory.parseString(
      s"""
      |akka.actor.guardian-supervisor-strategy = akka.actor.StoppingSupervisorStrategy
      """.stripMargin)
  }

  def actorCrashed(actorName: String, exc: Exception) {
    actorMappings.synchronized {
      if (!(actorMappings contains actorName)) {
        return
      }
    }

    crashedActors.synchronized {
      crashedActors += actorName
    }
    blockedActors = blockedActors + (actorName -> ("",""))
    // N.B. afterMessageReceive should be invoked after this.
  }

  // AspectJ runs into initialization problems if a new ActorSystem is created
  // by the constructor. Instead use a getter to create on demand.
  var _actorSystem : ActorSystem = null
  def actorSystem (config:Option[com.typesafe.config.Config]) : ActorSystem = {
    if (_actorSystem == null) {
      config match {
        case Some(c) =>
          _actorSystem = ActorSystem("new-system-" + counter, c)
        case None =>
          _actorSystem = ActorSystem("new-system-" + counter)
      }
      _random = new Random(0)
      scheduleFunctionRef = _actorSystem.actorOf(Props(classOf[ScheduleFunctionReceiver]),
        name="ScheduleFunctionPlaceholder")
      counter += 1
    }
    _actorSystem
  }

  def actorSystem () : ActorSystem = {
    return actorSystem(Some(defaultAkkaConfig))
  }

  def actorSystemInitialized: Boolean = _actorSystem != null

  private[this] var _random = new Random(0)
  def seededRandom() : Random = {
    return _random
  }
 
  
  def await_enqueue() {
    tellEnqueue.await()
  }

  // Whether to ignore (pass-through) all events
  def setPassthrough() {
    _passThrough.set(true)
  }

  def unsetPassthrough() {
    _passThrough.set(false)
  }

  /**
   * When an external thread (one not named .*dispatcher.*) wants to send
   * messages at a known (thread-safe) point, it should invoke this interface.
   *
   * If an external thread sends messages outside of this interface, its
   * messages will not be sent right away; instead they will be passed to
   * scheduler.enqueue_message() to be sent later at known point.
   *
   * N.B. send != deliver
   */
  def sendKnownExternalMessages(sendBlock: () => Any) {
    sendingKnownExternalMessages.synchronized {
      sendingKnownExternalMessages.set(true)
      sendBlock()
      sendingKnownExternalMessages.set(false)
    }
  }

  /**
   * Rather than having akka assign labels $a, $b, $c, etc to temp actors,
   * assign our own that will be more resilient to divergence in the execution
   * as we replay. See this design doc for more details:
   *   https://docs.google.com/document/d/1rAM8EEy3WnLRhhPROvHmBhAREv0rmihz0Gw0GgF1xC4
   */
  def assignTempPath(tempPath: ActorPath): ActorPath = synchronized {
    val callStack = Thread.currentThread().getStackTrace().map(e => e.getMethodName).drop(14) // drop off common prefix
    val min3 = math.min(3, callStack.length)
    var truncated = callStack.take(min3).toList
    if (idToPrependToCallStack != "") {
      truncated = idToPrependToCallStack :: truncated
      idToPrependToCallStack = ""
    }
    val bytes = truncated.mkString("-").getBytes(StandardCharsets.UTF_8)
    val b64 = java.util.Base64.getEncoder.encodeToString(bytes)
    val path = tempPath / b64
    return path
  }

  // In cases where there might be multiple threads with the same callstack
  // invoking `ask`, have them call this with an ID to avoid ambiguity in temp
  // actor names. Serialized with a lock on the Instrumenter object.
  var idToPrependToCallStack = ""

  // linearize == if there are concurrent asks, make sure only one happens at
  // a time.
  def linearizeAskWithID[E](id: String, ask: () => Future[E]) : Future[E] = synchronized {
    assert(idToPrependToCallStack == "")
    idToPrependToCallStack = id
    return ask()
  }

  def receiverIsAlive(receiver: ActorRef): Boolean = {
    // Drop any messages destined for actors that don't currently exist.
    // This is to prevent (tellEnqueue) deadlock within Instrumenter.
    def pathWithUid(): Iterator[String] = {
      // Lop off /user/ and the name
      val prefix = receiver.path.elements.drop(1).dropRight(1)
      val name = receiver.path.name + "#" + receiver.path.uid
      return (prefix.toVector :+ name).iterator
    }
    val isDead = (receiver.toString.contains("/user/") && _actorSystem != null &&
                  _actorSystem.asInstanceOf[ActorSystemImpl].guardian.getChild(pathWithUid) == Nobody)
    return !isDead
  }

  var _dispatchAfterMailboxIdle = ""
  val _dispatchAfterMailboxIdleLock = new Object

  def dispatchAfterMailboxIdle(actorName: String) {
    _dispatchAfterMailboxIdleLock.synchronized {
      assert(_dispatchAfterMailboxIdle == "")
      _dispatchAfterMailboxIdle = actorName
    }
  }

  def mailboxIdle(mbox: Mailbox) = {
    var shouldDispatch = false
    _dispatchAfterMailboxIdleLock.synchronized {
      if (_dispatchAfterMailboxIdle != "" && mbox.actor != null && mbox.actor.self.path.name == _dispatchAfterMailboxIdle) {
        println("mailboxIdle!: " + mbox.actor.self)
        _dispatchAfterMailboxIdle = ""
        shouldDispatch = true
        previousActor = ""
      }
    }
    if (shouldDispatch) {
      assert(!started.get)
      scheduler.handleMailboxIdle
    }
  }

  /**
   * Two cases:
   *  - In a normal `tell` from actor to actor, we need to mark
   *    tellEnqueue.tell() to let us know that there should later be a
   *    tellEnqueue.enqueue(). [See TellEnqueue class docs].
   *
   *  - If the thread doing the `tell` is external (outside of akka's thread
   *    pool and any of our schedulers), we can't let them send right
   *    now, since that won't be thread-safe. Instead, send it through
   *    scheduler.enqueue_message.
   */
  def tell(receiver: ActorRef, msg: Any, sender: ActorRef) : Boolean = {
    // Crucial property: if this is an external thread and STS2 is currently
    // sendingKnownExternalMessages, block here until STS2 is done!
    val sendingKnown = sendingKnownExternalMessages.synchronized {
      sendingKnownExternalMessages.get()
    }

    assert(receiver != null)

    if (!receiverIsAlive(receiver)) {
      logger.warn("Dropping message to non-existent receiver: " + sender +
        " -> " + receiver + " " + msg)
      return false
    }

    if (_passThrough.get()) {
      return true
    }

    if (Thread.currentThread.getName().startsWith("codeBlock-")) {
      codeBlockSends += ((receiver.path.name, msg))
    }

    // First check if it's an external thread sending outside of
    // sendKnownExternalMessages.
    if (!scheduler.isSystemCommunication(sender, receiver, msg) &&
        !Instrumenter.threadNameIsAkkaInternal() &&
        !sendingKnown &&
        receiver.path.parent.name != "temp") {
      scheduler.enqueue_message(Some(sender), receiver.path.name, msg)
      return false
    }

    // Now deal with normal messages.
    if (!scheduler.isSystemCommunication(sender, receiver, msg) &&
        receiver.path.parent.name != "temp") {
      if (logger.isTraceEnabled()) {
        logger.trace("tellEnqueue.tell(): " + sender + " -> " + receiver + " " + msg)
      }
      tellEnqueue.tell()
    }
    return true
  }
  
  def blockUntilPreStartCalled(ref: ActorRef) {
    preStartCalled.synchronized {
      while (!(preStartCalled contains ref)) {
        preStartCalled.wait
      }
      preStartCalled -= ref
    }
  }

  def preStartCalled(ref: ActorRef) {
    preStartCalled.synchronized {
      preStartCalled += ref
      preStartCalled.notifyAll
    }
  }
  
  // Callbacks for new actors being created
  def new_actor(system: ActorSystem, 
      props: Props, name: String, actor: ActorRef) : Unit = {

    if (_passThrough.get()) {
      return
    }

    if (actor.toString.contains("/system") || name == "/" || actor.path.name == "user") {
      return
    }
   
    val event = new SpawnEvent(currentActor, props, name, actor)
    scheduler.event_produced(event : SpawnEvent)
    scheduler.event_consumed(event)

    if (!started.get) {
      seenActors += ((system, (actor, props, name)))
    }
    
    actorMappings.synchronized {
      actorMappings(name) = actor
      actorMappings.notifyAll
    }
      
    println("System has created a new actor: " + actor.path.name)
  }
  
  
  def new_actor(system: ActorSystem, 
      props: Props, actor: ActorRef) : Unit = {
    new_actor(system, props, actor.path.name, actor)
  }
  
  def reset_cancellables() {
    for (task <- registeredCancellableTasks.filterNot(c => c.isCancelled)) {
      task.cancel()
    }
    registeredCancellableTasks.clear
    ongoingCancellableTasks.clear
    cancellableToTimer.clear
    timerToCancellable.clear
  }
  
  def reset_per_system_state() {
    actorMappings.clear()
    preStartCalled.clear()
    seenActors.clear()
    allowedEvents.clear()
    dispatchers.clear()
    crashedActors.clear()
    Util.logger.reset()
    blockedActors = Map[String, (String, Any)]()
    tempToParent = new HashMap[ActorPath, String]
    askAnswerNotYetScheduled = new HashSet[PromiseActorRef]
    sendingKnownExternalMessages = new AtomicBoolean(false)
    stopDispatch = new AtomicBoolean(false)
    interruptAllScheduleBlocks
  }

  def interruptAllScheduleBlocks() {
    codeBlockThreads.synchronized {
      codeBlockThreads.foreach {
        case t => t.interrupt
      }
      codeBlockThreads.clear
    }
  }

  // Restart the system:
  //  - Create a new actor system
  //  - Inform the scheduler that things have been reset
  //  - Run the first event to start the first actor
  //  - Send the first message received by this actor.
  //  This is all assuming that we don't control replay of main
  //  so this is a way to replay the first message that started it all.
  def reinitialize_system(sys: ActorSystem, argQueue: Queue[Any]) {
    require(scheduler != null)

    shutdownCallback()

    _actorSystem = ActorSystem("new-system-" + counter, defaultAkkaConfig)
    _random = new Random(0)
    counter += 1
    
    reset_cancellables
    reset_per_system_state
    
    println("Started a new actor system.")

    // This is safe, we have just started a new actor system (after killing all
    // the old ones we knew about), there should be no actors running and no 
    // dispatch calls outstanding. That said, it is really important that we not
    // have other sources of ActorSystems etc.
    started.set(false)
    tellEnqueue.reset()

    // Tell scheduler that we are done restarting and it should prepare
    // to start the system
    scheduler.start_trace()
    // Rely on scheduler to do the right thing from here on out
  }
  
  
  def shutdown_system(alsoRestart: Boolean) = {
    val allSystems = new HashMap[ActorSystem, Queue[Any]]
    for ((system, args) <- seenActors) {
      val argQueue = allSystems.getOrElse(system, new Queue[Any])
      argQueue.enqueue(args)
      allSystems(system) = argQueue
    }

    reset_cancellables
    for ((system, argQueue) <- allSystems) {
      println("Shutting down the actor system. " + argQueue.size)
      if (alsoRestart) {
        system.registerOnTermination(reinitialize_system(system, argQueue))
      }

      system.shutdown()

      println("Shut down the actor system. " + argQueue.size)
    }
  }

  // Signal to the instrumenter that the scheduler wants to restart the system
  def restart_system() = {
    println("Restarting system")
    shutdown_system(true)
  }
  
  
    // Called before a message is received
  def beforeMessageReceive(cell: ActorCell) {
    throw new Exception("not implemented")
  }
  
  // Called after the message receive is done.
  def afterMessageReceive(cell: ActorCell) {
    throw new Exception("not implemented")
  }
  
  
  // Called before a message is received
  def beforeMessageReceive(cell: ActorCell, msg: Any) {
    if (_passThrough.get()) {
      return
    }
    if (scheduler.isSystemMessage(
        cell.sender.path.name, 
        cell.self.path.name,
        msg)) return

    if (cell.system != _actorSystem) {
      // Somehow, bizzarely, afterMessageReceive can be invoked for actors
      // from prior actor systems, after they have been shutdown. This obviously throws a
      // huge wrench into our current dispatching loop.
      println("cell.system != _actorSystem")
      return
    }
   
    scheduler.before_receive(cell, msg)
    currentActor = cell.self.path.name
    assert(!inActor.get)
    inActor.set(true)
  }
  
  /**
   * Called when, within `receive`, an actor blocks by calling Await.result(),
   * usually on a future returned by `ask`.
   *
   * To make progress, dispatch a new message.
   */
  def actorBlocked() {
    if (_passThrough.get()) {
      return
    }

    if ((!Instrumenter.threadNameIsAkkaInternal) ||
        sendingKnownExternalMessages.get) {
      // External thread (or ScheduleBlock), so we don't care if it blocks.
      return
    }

    // Mark the current actor as blocked.
    val oldPendingDispatch = currentPendingDispatch.getAndSet(None)
    assert(!oldPendingDispatch.isEmpty)
    blockedActors = blockedActors + (currentActor -> oldPendingDispatch.get)

    assert(inActor.get)
    inActor.set(false)

    scheduler.schedule_new_message(blockedActors.keySet) match {
      // Note that dispatch_new_message is a non-blocking call; it hands off
      // the message to a new thread and returns immediately.
      case Some((new_cell, envelope)) =>
        val dst = new_cell.self.path.name
        if (blockedActors contains dst) {
          throw new IllegalArgumentException("schedule_new_message returned a " +
                                             "dst that is blocked: " + dst)
        }
        dispatch_new_message(new_cell, envelope)
      case None =>
        // Hopefully because the scheduler wanted to quiesce early!
        logger.warn("Actor " + currentActor +
                    " is blocked, yet there are no messages to schedule. " +
                    Thread.currentThread.getName)
        started.set(false)
        scheduler.notify_quiescence()
    }
  }
  
  // Called after the message receive is done.
  def afterMessageReceive(cell: ActorCell, msg: Any) {
    if (_passThrough.get()) {
      return
    }

    if (cell.system != _actorSystem) {
      // Somehow, bizzarely, afterMessageReceive can be invoked for actors
      // from prior actor systems, after they have been shutdown. This obviously throws a
      // huge wrench into our current dispatching loop.
      println("cell.system != _actorSystem")
      return
    }

    if (scheduler.isSystemMessage(
        cell.sender.path.name,
        cell.self.path.name,
        msg)) return

    if (logger.isTraceEnabled()) {
      logger.trace("afterMessageReceive: just finished: " + cell.sender.path.name + " -> " +
        cell.self.path.name + " " + msg)
      logger.trace("tellEnqueue.await()...")
    }

    tellEnqueue.await()

    logger.trace("done tellEnqueue.await()")

    val oldPendingDispatch = currentPendingDispatch.getAndSet(None)
    assert(!oldPendingDispatch.isEmpty)
    assert(oldPendingDispatch.get == (cell.self.path.name, msg),
      oldPendingDispatch.get + " " + (cell.self.path.name, msg))
    assert(inActor.get, "!inActor.get: " + Thread.currentThread.getName + " " + currentActor)
    inActor.set(false)
    previousActor = currentActor
    currentActor = ""

    stopDispatch.synchronized {
      if (stopDispatch.get()) {
        println("Stopping dispatch..")
        stopDispatch.set(false)
        started.set(false)
        return
      }
    }

    scheduler.after_receive(cell)

    scheduler.schedule_new_message(blockedActors.keySet) match {
      case Some((new_cell, envelope)) =>
        val dst = new_cell.self.path.name
        if (blockedActors contains dst) {
          throw new IllegalArgumentException("schedule_new_message returned a " +
                                             "dst that is blocked: " + dst)
        }
        dispatch_new_message(new_cell, envelope)
      case None =>
        counter += 1
        started.set(false)
        scheduler.notify_quiescence()
    }
  }

  // Return whether to allow the answer through or not
  def receiveAskAnswer(temp: PromiseActorRef, msg: Any, sender: ActorRef) : Boolean = {
    if (_passThrough.get()) {
      return true
    }

    if (!(tempToParent contains temp.path)) {
      // temp actor was spawned by an external thread
      return true
    }
    // Assume it's impossible for an internal actor to `ask` the outside world
    // anything (which would need to be the case if the external thread is now
    // answering).
    // TODO(cs): our current preStart heuristic can cause this to fail, when
    // preStart sends an answer to an `ask`. Wait
    // until we properly handle preStart before we uncomment this.
    // assert(Instrumenter.threadNameIsAkkaInternal)

    if (!(askAnswerNotYetScheduled contains temp)) {
      // Hasn't been scheduled for delivery yet.
      askAnswerNotYetScheduled += temp
      // Create a fake ActorCell and Envelope and give it to scheduler.
      val cell = new FakeCell(temp)
      val env = Envelope.apply(msg, sender, _actorSystem)
      scheduler.event_produced(cell, env)
      return false
    }
    // Else it was just scheduled for delivery immediately before this method
    // was called.
    askAnswerNotYetScheduled -= temp
    // Mark parent as unblocked.
    if (!(blockedActors contains tempToParent(temp.path))) {
      // Parent wasn't previously unblocked! This probably means that the
      // `ask` was initialized by a ScheduleBlock.
      // To keep the scheduling loop going, we need to dispatch again after
      // this answer has been delivered.
      dispatchAfterAskAnswer.set(true)
    } else {
      // We're about to wake up the parent who was previously blocked. Reset
      // currentPendingDispatch to what was previously pending when the parent
      // became blocked.
      val oldPendingDispatch = currentPendingDispatch.getAndSet(
        Some(blockedActors.get(tempToParent(temp.path)).get))
      blockedActors = blockedActors - tempToParent(temp.path)
      inActor.set(true)
    }
    currentActor = tempToParent(temp.path)
    tempToParent -= temp.path
    return true
  }

  def afterReceiveAskAnswer(temp: PromiseActorRef, msg: Any, sender: ActorRef) = {
    if (dispatchAfterAskAnswer.get) {
      println("Dispatching after receiveAskAnswer")
      inActor.set(false)
      currentPendingDispatch.set(None)
      dispatchAfterAskAnswer.set(false)
      scheduler.schedule_new_message(blockedActors.keySet) match {
        case Some((new_cell, envelope)) =>
          val dst = new_cell.self.path.name
          if (blockedActors contains dst) {
            throw new IllegalArgumentException("schedule_new_message returned a " +
                                               "dst that is blocked: " + dst)
          }
          dispatch_new_message(new_cell, envelope)
        case None =>
          counter += 1
          started.set(false)
          scheduler.notify_quiescence()
      }
    }
  }

  // Dispatch a message, i.e., deliver it to the intended recipient
  def dispatch_new_message(_cell: Cell, envelope: Envelope): Unit = {
    val snd = envelope.sender.path.name
    val rcv = _cell.self.path.name
    val msg = envelope.message

    if (_cell.self.isInstanceOf[PromiseActorRef]) {
      // This is an answer to an `ask`, and the scheduler just told us to
      // deliver it.
      // Go ahead and deliver it (receiveAskAnswer will be invoked)
      val tempRef = _cell.self.asInstanceOf[PromiseActorRef]
      tempRef.!(envelope.message)(envelope.sender)
      return
    }

    if (_cell.self == scheduleFunctionRef) {
      // This is a code block scheduled by akka.scheduler.schedule().
      // Run the code block rather than delivering any message.
      // Run it in a separate thread! Since it may block, e.g. by calling
      // `Await.result`
      // TODO(cs): shutdown this thread if it hasn't terminated, and the actor
      // system is also shutting down.
      val t = new Thread(new Runnable {
        def run() = {
          // If the timer is repeating, wait until the block is completed until
          // we retrigger it.
          msg.asInstanceOf[ScheduleBlock].apply()

          timerToCancellable.synchronized {
            if (timerToCancellable contains ("ScheduleFunction", msg)) {
              val cancellable = timerToCancellable(("ScheduleFunction", msg))
              if (ongoingCancellableTasks contains cancellable) {
                println("Retriggering repeating code block: " + msg)
                handleTick("ScheduleFunction", msg, cancellable)
              }
            }
          }
        }
      }, msg.asInstanceOf[ScheduleBlock].toString)

      codeBlockThreads.synchronized {
        codeBlockThreads += t
      }
      t.start()

      // Keep the scheduling loop going -- need to explicitly call
      // schedule_new_message, since afterMessageReceive will not be invoked.
      println("Dispatching after kicking off schedule block!")
      scheduler.schedule_new_message(blockedActors.keySet) match {
        case Some((new_cell, envelope)) =>
          val dst = new_cell.self.path.name
          if (blockedActors contains dst) {
            throw new IllegalArgumentException("schedule_new_message returned a " +
                                               "dst that is blocked: " + dst)
          }
          dispatch_new_message(new_cell, envelope)
        case None =>
          counter += 1
          started.set(false)
          scheduler.notify_quiescence()
      }
      return
    }

    Util.logger.mergeVectorClocks(snd, rcv)

    currentPendingDispatch.set(Some((rcv, msg)))

    // We now know that cell is a real ActorCell, not a FakeCell.
    val cell = _cell.asInstanceOf[ActorCell]
    
    allowedEvents += ((cell, envelope) : (ActorCell, Envelope))        

    val dispatcher = dispatchers.get(cell.self) match {
      case Some(value) => value
      case None => throw new Exception("internal error: " + cell.self + " " + msg)
    }
    
    scheduler.event_consumed(cell, envelope)
    if (codeBlockSends contains ((rcv, msg))) {
      codeBlockSends -= ((rcv, msg))
    }
    dispatcher.dispatch(cell, envelope)
    // Check if it was a repeating timer. If so, retrigger it.
    timerToCancellable.synchronized {
      if (timerToCancellable contains (rcv, msg)) {
        val cancellable = timerToCancellable((rcv, msg))
        if (ongoingCancellableTasks contains cancellable) {
          println("Retriggering repeating timer: " + rcv + " " + msg)
          handleTick(cell.self.path.name, msg, cancellable)
        }
      }
    }
  }

  def isTimer(rcv: String, msg: Any) : Boolean = {
    timerToCancellable.synchronized {
      return (timerToCancellable contains (rcv, msg)) ||
             rcv == "ScheduleFunctionPlaceholder" ||
             (codeBlockSends contains ((rcv, msg)))
    }
  }

  // Called when dispatch is called. One of two cases:
  //  - right after the message has first been `tell`ed but before the message
  //    is delivered. In this case we notify the scheduler that the message is
  //    pending, but don't allow the message to actually be delivered.
  //  - right after the scheduler has decided to deliver the message. In this
  //    case we let the message through.
  def aroundDispatch(dispatcher: MessageDispatcher, cell: ActorCell, 
      envelope: Envelope): Boolean = {
    if (_passThrough.get()) {
      return true
    }

    val value: (ActorCell, Envelope) = (cell, envelope)
    val receiver = cell.self
    val snd = envelope.sender.path.name
    val rcv = receiver.path.name

    // If this is a system message just let it through.
    if (scheduler.isSystemMessage(snd, rcv, envelope.message)) {
      return true
    }

    if (cell.system != _actorSystem) {
      // Somehow, bizzarely, afterMessageReceive can be invoked for actors
      // from prior actor systems, after they have been shutdown. This obviously throws a
      // huge wrench into our current dispatching loop.
      println("cell.system != _actorSystem")
      return false
    }

    // At this point, this should only ever be an internal thread.
    // TODO(cs): except, there is a bug where at the beginning of the
    // execution, the first sent message goes through aroundDispatch twice
    // (right as start_dispatch is invoked). Need to figure out why that is
    // before uncommenting this assert.
    // assert(Instrumenter.threadNameIsAkkaInternal || sendingKnownExternalMessages.get(),
    //   "external thread in aroundDispatch:" + snd + " -> " + rcv + " " + envelope.message)

    // Check it's an outgoing `ask` message, i.e. from a temp actor.
    // If so, do a bit of bookkeeping.
    // TODO(cs): assume that temp actors aren't used for anything other than
    // ask. Which probably isn't a good assumption.
    if (envelope.sender.path.parent.name == "temp" && currentActor != "") {
      tempToParent(envelope.sender.path) = currentActor
    }
    
        
    // If this is not a system message then check if we have already recorded
    // this event. Recorded => we are injecting this event (as opposed to some 
    // actor doing it in which case we need to report it)
    if (allowedEvents contains value) {
      allowedEvents.remove(value) match {
        case true => return true
        case false => throw new Exception("internal error")
      }
    }

    // Record the dispatcher for the current receiver.
    dispatchers(receiver) = dispatcher

    // Record that this event was produced. The scheduler is responsible for 
    // kick starting processing.
    scheduler.event_produced(cell, envelope)
    tellEnqueue.enqueue()
    return false
  }
  
  // Start scheduling and dispatching messages. This makes the scheduler responsible for
  // actually kickstarting things. 
  def start_dispatch() {
    assert(!started.get)
    started.set(true)
    println("start_dispatch. Dispatching!")
    scheduler.schedule_new_message(blockedActors.keySet) match {
      case Some((new_cell, envelope)) =>
        val dst = new_cell.self.path.name
        if (blockedActors contains dst) {
          throw new IllegalArgumentException("schedule_new_message returned a " +
                                             "dst that is blocked: " + dst)
        }
        dispatch_new_message(new_cell, envelope)
      case None =>
        counter += 1
        started.set(false)
        scheduler.notify_quiescence()
    }
  }

  // When someone calls akka.actor.schedulerOnce to schedule a Timer, we
  // record the returned Cancellable object here, so that we can cancel it later.
  def registerCancellable(c: Cancellable, ongoingTimer: Boolean,
                          receiver: String, msg: Any) {
    var _msg = msg

    timerToCancellable.synchronized {
      registeredCancellableTasks += c
      if (ongoingTimer) {
        ongoingCancellableTasks += c
      }
      if (receiver == "ScheduleFunction") {
        // Create a fake ActorCell and Envelope to give to scheduler.
        val cell = new FakeCell(scheduleFunctionRef)
        _msg = new ScheduleBlock(msg.asInstanceOf[Function0[Any]], cell)
      }
      cancellableToTimer(c) = ((receiver, _msg))
      // TODO(cs): for now, assume that msg's are unique. Don't assume that.
      if (timerToCancellable contains (receiver, _msg)) {
        throw new RuntimeException("Non-unique timer: "+ receiver + " " + _msg)
      }
      timerToCancellable((receiver, _msg)) = c
    }
    // Schedule it immediately!
    handleTick(receiver, _msg, c)
  }

  def removeCancellable(c: Cancellable) {
    timerToCancellable.synchronized {
      registeredCancellableTasks -= c
      val (receiver, msg) = cancellableToTimer(c)
      timerToCancellable -= ((receiver, msg))
      cancellableToTimer -= c
    }
  }

  // Invoked when a timer is sent
  def handleTick(receiver: String, msg: Any, c: Cancellable) {
    // println("handleTick " + receiver + " " + msg)
    if (!(registeredCancellableTasks contains c)) {
      throw new IllegalArgumentException("Cancellable " + (receiver, msg) +
                                         " is already cancelled...")
    }
    if (receiver == "ScheduleFunction") {
      val m = msg.asInstanceOf[ScheduleBlock]
      scheduler.enqueue_code_block(m.cell, m.envelope)
    } else {
      scheduler.enqueue_timer(receiver, msg)
    }
    if (!(ongoingCancellableTasks contains c)) {
      removeCancellable(c)
    }
  }

  def actorKnown(ref: ActorRef) : Boolean = {
    return actorMappings contains ref.path.name
  }

  def registerShutdownCallback(callback: ShutdownCallback) {
    shutdownCallback = callback
  }

  def registerCheckpointCallbacks(_applicationCheckpointCallback: CheckpointCallback,
                                  _restoreCheckpointCallback: RestoreCheckpointCallback) {
    checkpointCallback = _applicationCheckpointCallback
    restoreCheckpointCallback = _restoreCheckpointCallback
  }

  /**
   * If you're going to change out schedulers, call
   * `Instrumenter.scheduler = new_scheduler` *before* invoking this
   * method.
   */
  def checkpoint() : InstrumenterCheckpoint = {
    // TODO(cs): the shared state of the application is going to be reset on
    // reinitialize_system, due to shutdownCallback. This is problematic,
    // unless the application properly uses CheckpointSink's protocol for
    // checking invariants

    timerToCancellable.synchronized {
      val checkpoint = new InstrumenterCheckpoint(
        new HashMap[String, ActorRef] ++ actorMappings,
        new HashSet[(ActorSystem, Any)] ++ seenActors,
        new HashSet[(ActorCell, Envelope)] ++ allowedEvents,
        new HashMap[ActorRef, MessageDispatcher] ++ dispatchers,
        new HashMap[String, VectorClock] ++ Util.logger.actor2vc,
        _actorSystem,
        new HashMap[Cancellable, Tuple2[String, Any]] ++ cancellableToTimer,
        new HashSet[Cancellable] ++ ongoingCancellableTasks,
        new HashMap[Tuple2[String,Any], Cancellable] ++ timerToCancellable,
        checkpointCallback()
      )

      // Reset all state so that a new actor system can be started.
      registeredCancellableTasks.clear
      ongoingCancellableTasks.clear
      cancellableToTimer.clear
      timerToCancellable.clear
    }

    Util.logger.reset

    reinitialize_system(null, null)

    return checkpoint
  }

  /**
   * If you're going to change out schedulers, call
   * `Instrumenter.scheduler = old_scheduler` *before* invoking this
   * method.
   */
  def restoreCheckpoint(checkpoint: InstrumenterCheckpoint) {
    actorMappings.clear
    actorMappings ++= checkpoint.actorMappings
    seenActors.clear
    seenActors ++= checkpoint.seenActors
    allowedEvents.clear
    allowedEvents ++= checkpoint.allowedEvents
    dispatchers.clear
    dispatchers ++= checkpoint.dispatchers
    Util.logger.actor2vc = checkpoint.vectorClocks
    cancellableToTimer = checkpoint.cancellableToTimer
    ongoingCancellableTasks = checkpoint.ongoingCancellableTasks
    timerToCancellable = checkpoint.timerToCancellable
    registeredCancellableTasks.clear
    registeredCancellableTasks ++= cancellableToTimer.keys
    _actorSystem = checkpoint.sys
    restoreCheckpointCallback(checkpoint.applicationCheckpoint)
  }
}

object Instrumenter {
  var obj:Instrumenter = null
  def apply() = {
    if (obj == null) {
      obj = new Instrumenter
    }
    obj
  }

  val _overrideInternalThreadRule = new AtomicBoolean(false)

  // akka-dispatcher threads should invoke this when they want all message
  // send events to be treated as if they are coming from an external thread,
  // i.e. have message sends enqueued rather than sent immediately.
  def overrideInternalThreadRule() {
    assert(!_overrideInternalThreadRule.get)
    _overrideInternalThreadRule.set(true)
  }

  def unsetInternalThreadRuleOverride() {
    assert(_overrideInternalThreadRule.get)
    _overrideInternalThreadRule.set(false)
  }

  // Hack: check if name matches `.*dispatcher.*`, and moreover that the
  // dispatcher thread is not running asynchronously (out of our control) to
  // invoke an actor's preStart method. Hope that external
  // thread names don't match this pattern!
  def threadNameIsAkkaInternal() : Boolean = {
    return Thread.currentThread.getName().contains("dispatcher") &&
           !Thread.currentThread().getStackTrace().map(e => e.getMethodName).exists(e => e == "preStart") &&
           !_overrideInternalThreadRule.get()
  }

  // When a code block is about to be scheduled through
  // akka.scheduler.schedule, check if it's an akka internal code block.
  // TODO(cs): would be nice to have a better interface.
  def akkaInternalCodeBlockSchedule(): Boolean = {
    val callStack = Thread.currentThread().getStackTrace().map(e => e.getMethodName)
    // https://github.com/akka/akka/blob/release-2.2/akka-actor/src/main/scala/akka/pattern/AskSupport.scala#L334
    return callStack.contains("ask$extension")
  }
}

// Wraps a scala.function0 scheduled through akka.scheduler.schedule, but its
// toString tells us where it came from.
case class ScheduleBlock(f: Function0[Any], cell: Cell) {
  val callStack = getCallStack
  val envelope = Envelope.apply(this, null, Instrumenter()._actorSystem)

  def getCallStack(): String = {
    // TODO(cs): magic number 6 is brittle
    val callStack = Thread.currentThread.getStackTrace.drop(6)
    val min3 = math.min(3, callStack.length)
    var truncated = callStack.take(min3).toList
    return truncated.map(e => e.getFileName + ":" + e.getLineNumber).mkString("-")
  }

  override def toString(): String = {
    "ScheduleBlock-" + callStack
  }

  def apply(): Any = {
    return f()
  }
}
