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
  var checkpoints = new HashMap[String, CheckpointReply]

  def startCheckpointCollector(actorSystem: ActorSystem) {
    actorSystem.actorOf(Props[CheckpointSink], CheckpointSink.name)
  }

  def handleCheckpointResponse(checkpoint: Any, snd: String) {
    checkpoint match {
      case CheckpointReply(_) => checkpoints(snd) = checkpoint.asInstanceOf[CheckpointReply]
      case _ => throw new IllegalArgumentException("not a CheckpointReply: " + checkpoint)
    }
  }
}
