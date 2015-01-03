package akka.dispatch.verification


trait TestOracle {
  type Invariant = () => Boolean

  def setInvariant(invariant: Invariant)

  /**
   * Return whether there exists any execution containing the given external
   * events that triggers the given invariant.
   * At the end of the invocation, it is the responsibility of the TestOracle
   * to ensure that the ActorSystem is returned to a clean initial state.
   * Throws an IllegaleArgumentException if setInvariant has not been invoked.
   */
  def test(events: Seq[ExternalEvent]) : Boolean
}
