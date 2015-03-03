package sample;

import static java.lang.Thread.sleep;

import akka.dispatch.verification.Instrumenter;

import akka.actor.ActorRef;
import akka.actor.ScalaActorRef;
import akka.actor.Actor;
import akka.actor.Props;
import akka.actor.ActorCell;
import akka.actor.ActorSystem;
import akka.actor.ActorContext;
import akka.actor.Scheduler;
import akka.actor.Cancellable;
import akka.actor.LightArrayRevolverScheduler;

import akka.pattern.AskSupport;

import akka.dispatch.Envelope;
import akka.dispatch.MessageQueue;
import akka.dispatch.MessageDispatcher;

import scala.concurrent.impl.CallbackRunnable;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.ExecutionContext;
import java.lang.Runnable;

privileged public aspect WeaveActor {
  Instrumenter inst = Instrumenter.apply();
    
  pointcut enqueueOperation(MessageQueue me, ActorRef receiver, Envelope handle): 
  execution(public * akka.dispatch.MessageQueue.enqueue(ActorRef, Envelope)) &&
  args(receiver, handle) && this(me);
  
  Object around(MessageQueue me, ActorRef receiver, Envelope handle):
  enqueueOperation(me, receiver, handle) {
	return proceed(me, receiver, handle);
  }
  
  
  
  before(ActorCell me, Object msg):
  execution(* akka.actor.ActorCell.receiveMessage(Object)) &&
  args(msg, ..) && this(me) {
	inst.beforeMessageReceive(me, msg);
  }
  
  after(ActorCell me, Object msg):
  execution(* akka.actor.ActorCell.receiveMessage(Object)) &&
  args(msg, ..) && this(me) {
	inst.afterMessageReceive(me, msg);
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
  
  
  after(ActorSystem me, Props props) returning(ActorRef actor):
  execution(ActorRef akka.actor.ActorSystem.actorOf(Props)) &&
  args(props) && this(me) {
  	inst.new_actor(me, props, actor);
  }
  
  after(ActorSystem me, Props props, String name) returning(ActorRef actor):
  execution(ActorRef akka.actor.ActorSystem.actorOf(Props, String)) &&
  args(props, name) && this(me) {
  	inst.new_actor(me, props, name, actor);
  }


  
  after(ActorContext me, Props props) returning(ActorRef actor):
  execution(ActorRef akka.actor.ActorContext.actorOf(Props)) &&
  args(props) && this(me) {
  	inst.new_actor(me.system(), props, actor);
  }
  
  after(ActorContext me, Props props, String name) returning(ActorRef actor):
  execution(ActorRef akka.actor.ActorContext.actorOf(Props, String)) &&
  args(props, name) && this(me) {
  	inst.new_actor(me.system(), props, name, actor);
  }


  before(ActorRef me, Object msg, ActorRef sender):
  execution(* akka.actor.ScalaActorRef.$bang(Object, ActorRef)) &&
  args(msg, sender) && this(me) {
  	inst.tell(me, msg, sender);
  }
  
  before(ActorRef me, Object msg, ActorRef sender):
  execution(* akka.actor.ActorRef.tell(Object, ActorRef)) &&
  args(msg, sender, ..) && this(me) {
  }

  // Override akka.actor.Scheduler.schedulerOnce
  pointcut scheduleOnce(LightArrayRevolverScheduler me, FiniteDuration delay, ActorRef receiver, Object msg, ExecutionContext exc, ActorRef sender):
  execution(public * akka.actor.LightArrayRevolverScheduler.scheduleOnce(FiniteDuration,ActorRef,Object,ExecutionContext,ActorRef)) &&
  args(delay, receiver, msg, exc, sender) && this(me);

  // Never actually proceed(), just schedule our own timer, which does not use
  // ! directly, but instead calls enqueue_message..
  Object around(LightArrayRevolverScheduler me, FiniteDuration delay, ActorRef receiver, Object msg, ExecutionContext exc, ActorRef sender):
  scheduleOnce(me, delay, receiver, msg, exc, sender) {
    class MyRunnable implements java.lang.Runnable {
      ActorRef rcv;
      Object m;
      Instrumenter i;
      Cancellable c;
      boolean shouldRun;

      public MyRunnable(ActorRef receiver, Object msg, Instrumenter inst, boolean _shouldRun) {
        rcv = receiver;
        m = msg;
        i = inst;
        shouldRun = _shouldRun;
      }

      public void setCancellable(Cancellable cancellable) {
        c = cancellable;
      }

      public void run() {
        // Use essentially the same scheduleOnce implementation, but don't use !
        // See:
        //   https://github.com/akka/akka/blob/cb05725c1ec8a09e9bfd57dd093911dd41c7b288/akka-actor/src/main/scala/akka/actor/Scheduler.scala#L105
        if (shouldRun) {
          i.handleTick(rcv, m, c);
        }
      }
    }
    boolean shouldRun = inst.notify_timer_scheduled(sender, receiver, msg);
    MyRunnable runnable = new MyRunnable(receiver, msg, inst, shouldRun);
    Cancellable c = me.scheduleOnce(delay, runnable, exc);
    runnable.setCancellable(c);
    inst.registerCancellable(c, false, receiver, msg);
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
    class MyRunnable implements java.lang.Runnable {
      ActorRef rcv;
      Object m;
      Instrumenter i;
      Cancellable c;
      boolean shouldRun;

      public MyRunnable(ActorRef receiver, Object msg, Instrumenter inst, boolean _shouldRun) {
        rcv = receiver;
        m = msg;
        i = inst;
        shouldRun = _shouldRun;
      }

      public void setCancellable(Cancellable cancellable) {
        c = cancellable;
      }

      public void run() {
        // Use essentially the same scheduleOnce implementation, but don't use !
        // See:
        //   https://github.com/akka/akka/blob/cb05725c1ec8a09e9bfd57dd093911dd41c7b288/akka-actor/src/main/scala/akka/actor/Scheduler.scala#L105
        if (shouldRun) {
          i.handleTick(rcv, m, c);
        }
      }
    }
    boolean shouldRun = inst.notify_timer_scheduled(sender, receiver, msg);
    MyRunnable runnable = new MyRunnable(receiver, msg, inst, shouldRun);
    Cancellable c = me.schedule(delay, interval, runnable, exc);
    runnable.setCancellable(c);
    inst.registerCancellable(c, true, receiver, msg);
    return c;
  }
}
