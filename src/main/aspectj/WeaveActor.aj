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
	inst.beforeMessageReceive(me);
  }
  
  after(ActorCell me, Object msg):
  execution(* akka.actor.ActorCell.receiveMessage(Object)) &&
  args(msg, ..) && this(me) {
	inst.afterMessageReceive(me);
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

  /*
  // TODO(cs): the following pointcut is not applied, for a reason I do not
  // understand. Debug this later, since it will probably come in handy. 
  // For what it's worth, here is the decompiled byte code from
  // akka.actor.Scheduler.scala:

  $ javap Scheduler.class 
  Compiled from "Scheduler.scala"
  public abstract class akka.actor.Scheduler$class extends java.lang.Object{
      public static final akka.actor.Cancellable schedule(akka.actor.Scheduler, scala.concurrent.duration.FiniteDuration, scala.concurrent.duration.FiniteDuration, akka.actor.ActorRef, java.lang.Object, scala.concurrent.ExecutionContext, akka.actor.ActorRef);
      public static final akka.actor.Cancellable schedule(akka.actor.Scheduler, scala.concurrent.duration.FiniteDuration, scala.concurrent.duration.FiniteDuration, scala.Function0, scala.concurrent.ExecutionContext);
      public static final akka.actor.ActorRef schedule$default$6(akka.actor.Scheduler, scala.concurrent.duration.FiniteDuration, scala.concurrent.duration.FiniteDuration, akka.actor.ActorRef, java.lang.Object);
      public static final akka.actor.Cancellable scheduleOnce(akka.actor.Scheduler, scala.concurrent.duration.FiniteDuration, akka.actor.ActorRef, java.lang.Object, scala.concurrent.ExecutionContext, akka.actor.ActorRef);
      public static final akka.actor.Cancellable scheduleOnce(akka.actor.Scheduler, scala.concurrent.duration.FiniteDuration, scala.Function0, scala.concurrent.ExecutionContext);
      public static final akka.actor.ActorRef scheduleOnce$default$5(akka.actor.Scheduler, scala.concurrent.duration.FiniteDuration, akka.actor.ActorRef, java.lang.Object);
      public static void $init$(akka.actor.Scheduler);
  }

  // Override akka.actor.Scheduler.schedulerOnce
  // TODO(cs): also interpose on akka.actor.Scheduler.schedule()
  pointcut Timer(Scheduler me, FiniteDuration delay, ActorRef receiver, Object msg, ExecutionContext exc, ActorRef sender):
  execution(* akka.actor.Scheduler.schedulerOnce(..)) &&
  args(delay, receiver, msg, exc, sender) && this(me);

  // Never actually proceed(), just schedule our own timer, which does not use
  // ! directly, but instead calls enqueue_message.
  Object around(Scheduler me, FiniteDuration delay, ActorRef receiver, Object msg, ExecutionContext exc, ActorRef sender):
  Timer(me, delay, receiver, msg, exc, sender) {
    class MyRunnable implements java.lang.Runnable {
      ActorRef rcv;
      Object m;
      Instrumenter i;

      public MyRunnable(ActorRef receiver, Object msg, Instrumenter inst) {
        rcv = receiver;
        m = msg;
        i = inst;
      }

      public void run() {
        // Use essentially the same scheduleOnce implementation, but don't use !
        // See:
        //   https://github.com/akka/akka/blob/cb05725c1ec8a09e9bfd57dd093911dd41c7b288/akka-actor/src/main/scala/akka/actor/Scheduler.scala#L105
        i.handleTick(rcv, m);
      }
    }
    me.scheduleOnce(delay, new MyRunnable(receiver, msg, inst), exc);
	  return null;
  }
  */
}
