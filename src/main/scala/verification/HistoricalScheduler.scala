package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
import akka.actor.FSM.Timer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue

object HistoricalScheduler {
  type EventMapper = (Event) => Option[Event]
}

// A mix-in for any scheduler that somehow depends on recorded events.
// Primarily useful for application runners to make updates to recorded
// events. Those update may be needed in order to avoid
// bugs (e.g. updating the values of any recorded ActorRef's, which change between
// runs)
trait HistoricalScheduler {
  type EventMapper = (Event) => Option[Event]

  // User-specified event updater.
  var eventMapper : Option[EventMapper] = None

  // When FSM.Timer's are scheduled, store them here.
  // This is really only needed to deal with the non-serializability of Timer
  // events.
  var scheduledFSMTimers = new HashMap[TimerFingerprint, Timer]

  // Called by scheduler from notify_timer_scheduled 
  def handle_timer_scheduled(sender: ActorRef, receiver: ActorRef,
                             msg: Any, messageFingerprinter: FingerprintFactory) {
    val snd = if (sender == null) "deadLetters" else sender.path.name
    val rcv = if (receiver == null) "deadLetters" else receiver.path.name
    msg match {
      case Timer(name, nestedMsg, repeat, generation) =>
        val fingerprint = TimerFingerprint(name, snd, rcv,
          messageFingerprinter.fingerprint(nestedMsg), repeat, generation)
        scheduledFSMTimers(fingerprint) = msg.asInstanceOf[Timer]
      case _ =>
        // TODO(cs): not sure this is really necessary! We only need
        // scheduledFSMTimers to deal with non-serializability of Timers. As long
        // as this msg is serializable, there shouldn't be a problem?
        println("Warning: Non-akka.FSM.Timers not yet supported:" + msg)
    }
  }

  // Find a timer (any timer, not just FSM.Timer) that's waiting to be triggered
  // TODO(cs): enforce that timer t is sent less than k
  // times (where k is the number of times it was originally sent).
  def getRandomPendingTimer(): Option[(String, Any)] = {
    if (!Instrumenter().cancellableToTimer.isEmpty) {
      // Taking the first value gives us some degree of randomness
      return Some(Instrumenter().cancellableToTimer.values.head)
    }
    return None
  }

  def getAllPendingTimers(): Seq[(String, Any)] = {
    return new Queue[(String, Any)] ++ Instrumenter().cancellableToTimer.values
  }

  def setEventMapper(mapper: EventMapper) {
    eventMapper = Some(mapper)
  }

  def updateEvents(events: Seq[Event]) : Seq[Event] = {
    eventMapper match {
      case Some(f) =>
        return events.map(f).flatten
      case None => return events
    }
  }
}
