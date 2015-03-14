package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}
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
