package akka.dispatch.verification

import akka.actor.{ ActorCell, ActorRef, Props }
import akka.dispatch.{ Envelope }

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Semaphore


object IDGenerator {
  var uniqueId = new AtomicInteger

  def get() : Integer = {
    return uniqueId.incrementAndGet()
  }
}

       
case class Unique(
  val event : Event,
  var id : Int = IDGenerator.get()
)

abstract class Event

case class MsgEvent(
    sender: String, receiver: String, msg: Any) extends Event

case class SpawnEvent(
    parent: String, props: Props, name: String, actor: ActorRef) extends Event



// Base class for failure detector messages
abstract class FDMessage

// Failure detector node information
case class FailureDetectorOnline(fdNode: String) extends FDMessage

// A node is unreachable, either due to node failure or partition.
case class NodeUnreachable(actor: String) extends FDMessage

// A new node is now reachable, either because a partition healed or an actor spawned.
case class NodeReachable(actor: String) extends FDMessage

// Query the failure detector for currently reachable actors.
case object QueryReachableGroup extends FDMessage

// Response to failure detector queries.
case class ReachableGroup(actors: Array[String]) extends FDMessage

trait TellEnqueue {
  def tell()
  def enqueue()
  def reset()
  def await ()
}

class TellEnqueueBusyWait extends TellEnqueue {
  
  var enqueue_count = new AtomicInteger
  var tell_count = new AtomicInteger
  
  def tell() {
    tell_count.incrementAndGet()
  }
  
  def enqueue() {
    enqueue_count.incrementAndGet()
  }
  
  def reset() {
    tell_count.set(0)
    enqueue_count.set(0)
  }

  def await () {
    while (tell_count.get != enqueue_count.get) {}
  }
  
}
    

class TellEnqueueSemaphore extends Semaphore(1) with TellEnqueue {
  
  var enqueue_count = new AtomicInteger
  var tell_count = new AtomicInteger
  
  def tell() {
    tell_count.incrementAndGet()
    reducePermits(1)
    require(availablePermits() <= 0)
  }

  def enqueue() {
    enqueue_count.incrementAndGet()
    require(availablePermits() <= 0)
    release()
  }
  
  def reset() {
    tell_count.set(0)
    enqueue_count.set(0)
    // Set available permits to 0
    drainPermits() 
    // Add a permit
    release()
  }
  
  def await() {
    acquire
    release
  }
}


