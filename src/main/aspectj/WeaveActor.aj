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

import akka.pattern.AskSupport;

import akka.dispatch.Envelope;
import akka.dispatch.MessageQueue;
import akka.dispatch.MessageDispatcher;

import scala.concurrent.impl.CallbackRunnable;



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
}
