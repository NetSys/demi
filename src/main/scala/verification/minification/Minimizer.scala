package akka.dispatch.verification

trait Minimizer {
  def minimize(events: Seq[ExternalEvent]) : Seq[ExternalEvent]
}
