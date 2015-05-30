package akka.dispatch.verification

import akka.actor.{Cell, ActorRef}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

/**
 * Schedules events in a round-robin fashion.
 */
class FairScheduler extends AbstractScheduler {
  var actorQueue = new Queue[String]
  var index = 0

  // Current set of enabled events.
  val pendingEvents = new HashMap[String, Queue[Uniq[(Cell, Envelope)]]]  

  def event_produced(cell: Cell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[Uniq[(Cell, Envelope)]])
    
    val uniq = Uniq[(Cell, Envelope)]((cell, envelope))
    pendingEvents(rcv) = msgs += uniq
    // Start dispatching events
    if (!instrumenter.started.get) {
      instrumenter.start_dispatch()
    }
  }

  def nextActor (blockedActors: Set[String]) : String = {
    if (actorQueue.size == 0) {
      actorQueue = actorQueue ++ actorNames.toList
    }
    var next = actorQueue(index)
    index = (index + 1) % actorQueue.size
    // Assume: never the case that all actors are blocked at the same time.
    while (!(blockedActors contains next)) {
      next = actorQueue(index)
      index = (index + 1) % actorQueue.size
    }
    next
  }
  
  def find_message_to_schedule(blockedActors: Set[String]) : Option[Uniq[(Cell, Envelope)]] = {
    val receiver = nextActor(blockedActors)
    // Do we have some pending events
    if (pendingEvents.isEmpty) {
      None
    } else { 
      pendingEvents.get(receiver) match {
        case Some(queue) =>
          if (queue.isEmpty == true) {
            pendingEvents.remove(receiver) match {
              case Some(key) => find_message_to_schedule(blockedActors)
              case None => throw new Exception("Internal error") // Really this is saying pendingEvents does not have 
                                                                 // receiver as a key
            }
          } else {
            val uniq = queue.dequeue()
            Some(uniq)
          }
        case None =>
          find_message_to_schedule(blockedActors)
      }
    }
  }
  
  // Figure out what is the next message to schedule.
  def schedule_new_message(blockedActors: Set[String]) : Option[(Cell, Envelope)] = {
    find_message_to_schedule(blockedActors) match {
      case Some(uniq) =>
        return Some(uniq.element)
      case None =>
        return None
    }
  }
  
  def enqueue_message(receiver: String, msg: Any) {
    throw new Exception("NYI")
  }

  override def notify_quiescence () {
    println("No more messages to process " + pendingEvents)
  }

  def notify_timer_cancel(receiver: ActorRef, msg: Any) {
    val rcv = receiver.path.name
    pendingEvents(rcv).dequeueFirst(tuple => tuple.element._2.message == msg)
    if (pendingEvents(rcv).isEmpty) {
      pendingEvents -= rcv
    }
  }

  override def reset_all_state () = {
    super.reset_all_state
    index = 0
    pendingEvents.clear
    actorQueue.clear
  }
}
