package sample;

import static java.lang.Thread.sleep;

import akka.dispatch.verification.Instrumenter;
import akka.dispatch.verification.WrappedCancellable;
import akka.dispatch.verification.ShutdownHandler;

import akka.actor.ActorRef;
import akka.actor.ScalaActorRef;
import akka.actor.InternalActorRef;
import akka.actor.Actor;
import akka.actor.Props;
import akka.actor.ActorCell;
import akka.actor.ActorSystem;
import akka.actor.ActorSystemImpl;
import akka.actor.ActorContext;
import akka.actor.Scheduler;
import akka.actor.Cancellable;
import akka.actor.LightArrayRevolverScheduler;
import akka.actor.LocalActorRefProvider;
import akka.actor.VirtualPathContainer;
import akka.actor.ActorPath;

import akka.pattern.AskSupport;
import akka.pattern.PromiseActorRef;

import akka.dispatch.Envelope;
import akka.dispatch.MessageQueue;
import akka.dispatch.MessageDispatcher;
import akka.dispatch.Mailbox;
import akka.dispatch.MonitorableThreadFactory;

import scala.concurrent.impl.CallbackRunnable;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.ExecutionContext;
import scala.Function0;
import scala.runtime.BoxedUnit;
import java.lang.Runnable;

privileged public aspect WeaveActor {
  Instrumenter inst = Instrumenter.apply();

  // Don't allow LightArrayRevolverScheduler to restart its timer thread.
  // TODO(cs): much cleaner would be to not use Thread.stop(), and figure out
  // what's really causing this memory leak...
  Object around(MonitorableThreadFactory me):
  execution(* akka.dispatch.MonitorableThreadFactory.newThread(Runnable)) &&
  this(me) {
    if (me.name().contains("scheduler") && me.counter().get() > 1) {
      return null;
    }
    return proceed(me);
  }

  // Prevent memory leaks when the ActorSystem crashes?
  Object around(ActorSystemImpl me):
  execution(* akka.actor.ActorSystemImpl.uncaughtExceptionHandler(..)) &&
  this(me) {
    return ShutdownHandler.getHandler(me);
  }

  before(ActorCell me, Object msg):
  execution(* akka.actor.ActorCell.receiveMessage(Object)) &&
  args(msg, ..) && this(me) {
    inst.beforeMessageReceive(me, msg);
  }

  // N.B. the order of the next two advice is important: need to catch throws
  // before after
  after(ActorCell me, Object msg) throwing (Exception ex):
  execution(* akka.actor.ActorCell.receiveMessage(Object)) &&
  args(msg, ..) && this(me) {
    inst.actorCrashed(me.self().path().name(), ex);
  }

  after(ActorCell me, Object msg):
  execution(* akka.actor.ActorCell.receiveMessage(Object)) &&
  args(msg, ..) && this(me) {
    inst.afterMessageReceive(me, msg);
  }

  after(Mailbox me):
  execution(* akka.dispatch.Mailbox.run()) && this(me) {
    inst.mailboxIdle(me);
  }

  pointcut dispatchOperation(MessageDispatcher me, ActorCell receiver, Envelope handle):
  execution(* akka.dispatch.MessageDispatcher.dispatch(..)) &&
  args(receiver, handle, ..) && this(me);

  Object around(MessageDispatcher me, ActorCell receiver, Envelope handle):
  dispatchOperation(me, receiver, handle) {
    if (inst.aroundDispatch(me, receiver, handle))
      return proceed(me, receiver, handle);
    else
      return null;
  }

  // Interposition on `ask`. See this doc for more details:
  // https://docs.google.com/document/d/1_LUceHvQoamlBtNNbqA4CxBH-zvUKlZhnSjLwTc16q4/edit#
  pointcut receiveAnswer(PromiseActorRef me, Object message, ActorRef sender):
  execution(public void akka.pattern.PromiseActorRef.$bang(Object, ActorRef)) &&
  args(message, sender) && this(me);

  Object around(PromiseActorRef me, Object message, ActorRef sender):
  receiveAnswer(me, message, sender) {
    if (inst.receiveAskAnswer(me, message, sender)) {
      Object ret = proceed(me, message, sender);
      inst.afterReceiveAskAnswer(me, message, sender);
      return ret;
    }
    return null;
  }

  // Sanity check: make sure we don't allocate two temp actors at the same
  // time with the same name.
  before(VirtualPathContainer me, String name, InternalActorRef ref):
  execution(* VirtualPathContainer.addChild(String, InternalActorRef)) &&
  this(me) && args(name, ref) {
    if (me.children.containsKey(name)) {
      System.err.println("WARNING: temp name already taken: " + name);
    }
  }

  /*
   For debugging redundant temp names:
  before(VirtualPathContainer me, String name, InternalActorRef ref):
  execution(* VirtualPathContainer.removeChild(String, InternalActorRef)) &&
  this(me) && args(name, ref) {
    System.out.println("removeChild: " + name);
  }

  before(VirtualPathContainer me, String name):
  execution(* VirtualPathContainer.removeChild(String)) &&
  this(me) && args(name) {
    System.out.println("removeChild: " + name);
  }
  */

  // Interposition on the code that assigns IDs to temporary actors. See this
  // doc for more details:
  // https://docs.google.com/document/d/1rAM8EEy3WnLRhhPROvHmBhAREv0rmihz0Gw0GgF1xC4/edit#
  pointcut tempPath(LocalActorRefProvider me):
  execution(public ActorPath LocalActorRefProvider.tempPath()) && this(me);

  Object around(LocalActorRefProvider me): tempPath(me) {
    return inst.assignTempPath(me.tempNode);
  }

  // newActor is invoked before preStart is called!
  after(ActorCell me):
  execution(* akka.actor.ActorCell.newActor()) &&
  this(me) {
    inst.new_actor(me.system(), me.props, me.self);
  }

  // after preStart has been called, i.e. actor is ready to receive messages.
  after(ActorCell me):
  execution(* akka.actor.ActorCell.create(..)) &&
  this(me) {
    inst.preStartCalled(me.self);
  }

  // Block until the actor has actually been created and preStart has been invoked!
  // (which is done asynchronously)
  after(ActorSystem me, Props props) returning(ActorRef actor):
  execution(ActorRef akka.actor.ActorSystem.actorOf(Props)) &&
  args(props) && this(me) {
    inst.blockUntilPreStartCalled(actor);
  }

  // Block until the actor has actually been created and preStart has been invoked!
  // (which is done asynchronously)
  after(ActorSystem me, Props props, String name) returning(ActorRef actor):
  execution(ActorRef akka.actor.ActorSystem.actorOf(Props, String)) &&
  args(props, name) && this(me) {
    inst.blockUntilPreStartCalled(actor);
  }

  // Block until the actor has actually been created and preStart has been invoked!
  // (which is done asynchronously)
  after(ActorContext me, Props props) returning(ActorRef actor):
  execution(ActorRef akka.actor.ActorContext.actorOf(Props)) &&
  args(props) && this(me) {
    inst.blockUntilPreStartCalled(actor);
  }

  // Block until the actor has actually been created and preStart has been invoked!
  // (which is done asynchronously)
  after(ActorContext me, Props props, String name) returning(ActorRef actor):
  execution(ActorRef akka.actor.ActorContext.actorOf(Props, String)) &&
  args(props, name) && this(me) {
    inst.blockUntilPreStartCalled(actor);
  }

  Object around(ActorRef me, Object msg, ActorRef sender):
  execution(* akka.actor.ScalaActorRef.$bang(Object, ActorRef)) &&
  args(msg, sender) && this(me) {
    if (inst.tell(me, msg, sender)) {
      return proceed(me, msg, sender);
    }
    return null;
  }

  // Override akka.actor.Scheduler.schedulerOnce
  pointcut scheduleOnce(LightArrayRevolverScheduler me, FiniteDuration delay, ActorRef receiver, Object msg, ExecutionContext exc, ActorRef sender):
  execution(public * akka.actor.LightArrayRevolverScheduler.scheduleOnce(FiniteDuration,ActorRef,Object,ExecutionContext,ActorRef)) &&
  args(delay, receiver, msg, exc, sender) && this(me);

  // Never actually proceed(), just schedule our own timer, which does not use
  // ! directly, but instead calls enqueue_message..
  Object around(LightArrayRevolverScheduler me, FiniteDuration delay, ActorRef receiver, Object msg, ExecutionContext exc, ActorRef sender):
  scheduleOnce(me, delay, receiver, msg, exc, sender) {
    if (!inst.actorKnown(receiver)) {
      return proceed(me,delay,receiver,msg,exc,sender);
    }
    class MyRunnable implements java.lang.Runnable {
      // Make it a no-op!
      public void run() {
      }
    }
    MyRunnable runnable = new MyRunnable();
    Cancellable c = new WrappedCancellable(
      me.scheduleOnce(delay, runnable, exc), receiver.path().name(), msg);
    inst.registerCancellable(c, false, receiver.path().name(), msg);
    return c;
  }

  // Override akka.actor.Scheduler.scheduler
  pointcut schedule(LightArrayRevolverScheduler me, FiniteDuration delay, FiniteDuration interval, ActorRef receiver, Object msg, ExecutionContext exc, ActorRef sender):
  execution(public * akka.actor.LightArrayRevolverScheduler.schedule(FiniteDuration,FiniteDuration,ActorRef,Object,ExecutionContext,ActorRef)) &&
  args(delay, interval, receiver, msg, exc, sender) && this(me);

  // Never actually proceed(), just schedule our own timer, which does not use
  // ! directly, but instead calls enqueue_message..
  Object around(LightArrayRevolverScheduler me, FiniteDuration delay, FiniteDuration interval, ActorRef receiver, Object msg, ExecutionContext exc, ActorRef sender):
  schedule(me, delay, interval, receiver, msg, exc, sender) {
    if (!inst.actorKnown(receiver)) {
      return proceed(me,delay,interval,receiver,msg,exc,sender);
    }
    class MyRunnable implements java.lang.Runnable {
      // Make it a no-op!
      public void run() {
      }
    }
    MyRunnable runnable = new MyRunnable();
    Cancellable c = new WrappedCancellable(
      me.schedule(delay, interval, runnable, exc), receiver.path().name(), msg);
    inst.registerCancellable(c, true, receiver.path().name(), msg);
    return c;
  }

  // Override akka.actor.Scheduler.scheduler(block)
  pointcut scheduleBlock(LightArrayRevolverScheduler me, FiniteDuration delay, FiniteDuration interval, scala.Function0<scala.runtime.BoxedUnit> block, ExecutionContext exc):
  execution(public * schedule(scala.concurrent.duration.FiniteDuration, scala.concurrent.duration.FiniteDuration, scala.Function0<scala.runtime.BoxedUnit>, scala.concurrent.ExecutionContext)) &&
  args(delay, interval, block, exc) && this(me);

  // Wrap the function in a special "Message" wrapper, that appears to
  // STS2 schedulers like a message, but which the Instrumenter understands as a
  // function to be invoked rather than a message to be sent.
  // TODO(cs): issue: no receiver.
  Object around(LightArrayRevolverScheduler me, FiniteDuration delay, FiniteDuration interval, scala.Function0<scala.runtime.BoxedUnit> block, ExecutionContext exc):
  scheduleBlock(me, delay, interval, block, exc) {
    if (!inst.actorSystemInitialized() || Instrumenter.akkaInternalCodeBlockSchedule()) {
      return proceed(me, delay, interval, block, exc);
    }

    class MyRunnable implements java.lang.Runnable {
      // Make it a no-op!
      public void run() {
      }
    }
    MyRunnable runnable = new MyRunnable();
    Cancellable c = new WrappedCancellable(me.schedule(delay, interval, runnable, exc), "ScheduleFunction", block);
    inst.registerCancellable(c, true, "ScheduleFunction", block);
    return c;
  }

  // Override akka.actor.Scheduler.scheduler(block)
  pointcut scheduleOnceBlock(LightArrayRevolverScheduler me, FiniteDuration delay, scala.Function0<scala.runtime.BoxedUnit> block, ExecutionContext exc):
  execution(public * scheduleOnce(scala.concurrent.duration.FiniteDuration, scala.Function0<scala.runtime.BoxedUnit>, scala.concurrent.ExecutionContext)) &&
  args(delay, block, exc) && this(me);

  // Wrap the function in a special "Message" wrapper, that appears to
  // STS2 schedulers like a message, but which the Instrumenter understands as a
  // function to be invoked rather than a message to be sent.
  // TODO(cs): issue: no receiver.
  Object around(LightArrayRevolverScheduler me, FiniteDuration delay, scala.Function0<scala.runtime.BoxedUnit> block, ExecutionContext exc):
  scheduleOnceBlock(me, delay, block, exc) {
    if (!inst.actorSystemInitialized()) {
      return proceed(me, delay, block, exc);
    }

    class MyRunnable implements java.lang.Runnable {
      // Make it a no-op!
      public void run() {
      }
    }
    MyRunnable runnable = new MyRunnable();
    if (Instrumenter.akkaInternalCodeBlockSchedule()) {
      // Don't ever allow the `ask` timer to expire
      return me.scheduleOnce(delay, runnable, exc);
    }
    Cancellable c = new WrappedCancellable(me.scheduleOnce(delay, runnable, exc), "ScheduleFunction", block);
    inst.registerCancellable(c, false, "ScheduleFunction", block);
    return c;
  }
}
