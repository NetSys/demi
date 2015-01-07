package akka.dispatch.verification

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

/*

A utility class that can be used by schedulers to control failure detection
events.

FDMessageOrchestrator allows schedulers to:
  - register actors with the failure detector upon spawn.

A scheduler that makes use of a FDMessageOrchestrator allows actors to:
  - be informed of failure detection events. Actors simply need to accept
    NodeReachable and NodeUnreachable messages to be informed.
  - Actors can also query the failure detector for all reachable nodes by calling:
    actor.contextFor("../" + FailureDetector.fdName) ? QueryReachableGroup
    and then later accepting a ReachableGroup message.
*/

object FailureDetector {
  var fdName = "failure_detector"
}

// A fake actor, used as a placeholder to which failure detector requests can be sent.
// The scheduler intercepts messages sent to this actor.
class FailureDetector () extends Actor {
  def receive = {
    // This should never be called
    case _ => assert(false)
  }
}

class FDMessageOrchestrator (sched: Scheduler) {
  // Failure detector information
  // For each actor, track the set of other actors who are reachable.
  val fdState = new HashMap[String, HashSet[String]]
  val activeActors = new HashSet[String]
  val pendingFDRequests = new Queue[String]

  def clear() {
    fdState.clear
    activeActors.clear
    pendingFDRequests.clear
  }

  def startFD(actorSystem: ActorSystem) {
    actorSystem.actorOf(Props[FailureDetector], FailureDetector.fdName)
  }

  def create_node(node: String) {
    fdState(node) = new HashSet[String]
  }

  def isolate_node(node: String) {
    fdState(node).clear()
  }

  def unisolate_node(node: String) {
    fdState(node) ++= activeActors
  }

  def handle_start_event(node: String) {
    // Send FD message before adding an actor
    informFailureDetectorLocation(node)
    informNodeReachable(node)
    activeActors += node
  }

  def handle_kill_event(node: String) {
    activeActors -= node
    // Send FD message after removing the actor
    informNodeUnreachable(node)
  }

  def handle_partition_event(a: String, b: String) {
    // Send FD information to each of the actors
    informNodeUnreachable(a, b)
    informNodeUnreachable(b, a)
  }

  def handle_unpartition_event(a: String, b: String) {
    // Send FD information to each of the actors
    informNodeReachable(a, b)
    informNodeReachable(b, a)
  }

  def handle_fd_message(msg: Any, snd: String) {
    msg match {
      case QueryReachableGroup =>
        // Allow queries to be made during class initialization (more than one might be happening at
        // a time)
        pendingFDRequests += snd
      case _ =>
        assert(false)
    }
  }

  def send_all_pending_responses() {
    for (receiver <- pendingFDRequests) {
      answerFdQuery(receiver)
    }
    pendingFDRequests.clear
  }

  private[this] def informFailureDetectorLocation(actor: String) {
    sched.enqueue_message(actor, FailureDetectorOnline(FailureDetector.fdName))
  }

  private[this] def informNodeReachable(actor: String, node: String) {
    sched.enqueue_message(actor, NodeReachable(node))
    fdState(actor) += node
  }

  private[this] def informNodeReachable(node: String) {
    for (actor <- activeActors) {
      informNodeReachable(actor, node)
    }
  }

  private[this] def informNodeUnreachable(actor: String, node: String) {
    sched.enqueue_message(actor, NodeUnreachable(node))
    fdState(actor) -= node
  }

  private[this] def informNodeUnreachable(node: String) {
    for (actor <- activeActors) {
      informNodeUnreachable(actor, node)
    }
  }

  private[this] def answerFdQuery(sender: String) {
    // Compute the message
    val msg = ReachableGroup(fdState(sender).toSet)
    // Send failure detector information
    sched.enqueue_message(sender, msg)
  }
}
