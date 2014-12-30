package akka.dispatch.verification

trait Minimizer {
  def minimize(events: List[ExternalEvent]) : List[ExternalEvent]
}
