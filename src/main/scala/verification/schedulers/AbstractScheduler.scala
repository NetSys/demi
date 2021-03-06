package akka.dispatch.verification

import akka.actor.{Cell, ActorRef}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

// Basically just keeps track of actor names.
//
// Subclasses need to at least implement:
//   - schedule_new_message
//   - enqueue_message
//   - event_produced(cell: Cell, envelope: Envelope)
abstract class AbstractScheduler extends Scheduler {

  val logger = LoggerFactory.getLogger("AbstractScheduler")

  var instrumenter = Instrumenter()
  var currentTime = 0


  var actorNames = new HashSet[String]


  // Is this message a system message
  def isSystemCommunication(sender: ActorRef, receiver: ActorRef): Boolean = {
    if (sender == null && receiver == null) return true
    val senderPath = if (sender != null) sender.path.name else "deadLetters"
    val receiverPath = if (receiver != null) receiver.path.name else "deadLetters"
    return isSystemMessage(senderPath, receiverPath)
  }

  def isSystemMessage(src: String, dst: String): Boolean = {
    if ((actorNames contains src) || (actorNames contains dst))
      return dst == "deadLetters"

    return true
  }

  // Notification that the system has been reset
  def start_trace() : Unit = {
  }


  // Record that an event was consumed
  def event_consumed(event: Event) = {
  }

  def event_consumed(cell: Cell, envelope: Envelope) = {
  }

  // Record that an event was produced
  def event_produced(event: Event) = {
    event match {
      case event : SpawnEvent =>
        if (!(actorNames contains event.name)) {
          logger.debug("Sched knows about actor " + event.name)
          actorNames += event.name
        }
    }
  }


  // Called before we start processing a newly received event
  def before_receive(cell: Cell) {
    currentTime += 1
  }

  // Called after receive is done being processed
  def after_receive(cell: Cell) {
  }

  def notify_quiescence () {
  }

  def shutdown() {
    instrumenter.restart_system
  }

  // Get next event
  def next_event() : Event = {
    throw new Exception("NYI")
  }

  def reset_all_state () = {
    actorNames = new HashSet[String]
    currentTime = 0
  }
}
