package akka.dispatch.verification


trait TestOracle {
  type Invariant = (Seq[ExternalEvent]) => Boolean

  def setInvariant(invariant: Invariant)

  /**
   * Return whether there exists any execution containing the given external
   * events that triggers the given invariant.
   *
   * At the end of the invocation, it is the responsibility of the TestOracle
   * to ensure that the ActorSystem is returned to a clean initial state.
   * Throws an IllegalArgumentException if setInvariant has not been invoked.
   */
  def test(events: Seq[ExternalEvent]) : Boolean
}

object StatelessTestOracle {
  type OracleConstructor = () => TestOracle
}

/*
 * A TestOracle that throws away all state between invocations of test().
 * Useful for debugging or avoiding statefulness problems, e.g. deadlocks.
 */
// TODO(cs): there is currently a deadlock in PeekScheduler: if you invoke
// PeekScheduler.test() multiple times, it runs the first execution just fine,
// but the second execution never reaches Quiescence, and blocks infinitely
// trying to acquire traceSem. Figure out why.
class StatelessTestOracle(oracle_ctor: StatelessTestOracle.OracleConstructor) extends TestOracle {
  var invariant : Invariant = null

  def setInvariant(inv: Invariant) = {
    invariant = inv
  }

  def test(events: Seq[ExternalEvent]) : Boolean = {
    val oracle = oracle_ctor()
    try {
      Instrumenter().scheduler = oracle.asInstanceOf[Scheduler]
    } catch {
      case e: Exception => println("oracle not a scheduler?")
    }
    oracle.setInvariant(invariant)
    val result = oracle.test(events)
    Instrumenter().restart_system
    return result
  }
}
