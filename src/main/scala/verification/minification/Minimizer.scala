package akka.dispatch.verification

trait Minimizer {
  def minimize(events: Seq[ExternalEvent], violation_fingerprint: ViolationFingerprint) : Seq[ExternalEvent]
}
