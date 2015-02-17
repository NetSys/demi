package akka.dispatch.verification

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.collection.mutable.HashMap

// For checking invariants on application state:
final case object CheckpointRequest
final case class CheckpointReply(data: Any)

object CheckpointSink {
  var name = "checkpoint_sink"
}

// A fake actor, used as a placeholder to which checkpoint responses can be sent.
// The scheduler intercepts messages sent to this actor.
class CheckpointSink() extends Actor {
  def receive = {
    // This should never be called
    case _ => assert(false)
  }
}

class CheckpointCollector {
  // If a node is crashed, the value will be None rather than
  // Some(CheckpointReply)
  var checkpoints = new HashMap[String, Option[CheckpointReply]]
  var expectedResponses = 0

  def startCheckpointCollector(actorSystem: ActorSystem) {
    actorSystem.actorOf(Props[CheckpointSink], CheckpointSink.name)
  }

  // Pre: prepareRequests was recently invoked.
  def done() : Boolean = {
    return checkpoints.size == expectedResponses
  }

  def prepareRequests(actorRefs: Seq[ActorRef]) : Seq[(String, Any)] = {
    checkpoints.clear()
    // TODO(cs): I assume that crashed == ref.isTerminated... double check
    // this!
    val crashedActors = actorRefs.filter(ref => ref.isTerminated)
    for (crashed <- crashedActors) {
      checkpoints(crashed.path.name) = None
    }
    expectedResponses = actorRefs.length - crashedActors.length
    return actorRefs.filterNot(ref => ref.isTerminated).
                     map(ref => ((ref.path.name, CheckpointRequest)))
  }

  def handleCheckpointResponse(checkpoint: Any, snd: String) {
    checkpoint match {
      case CheckpointReply(_) => checkpoints(snd) = Some(checkpoint.asInstanceOf[CheckpointReply])
      case _ => throw new IllegalArgumentException("not a CheckpointReply: " + checkpoint)
    }
  }
}
