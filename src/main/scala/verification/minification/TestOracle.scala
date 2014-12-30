package akka.dispatch.verification


trait TestOracle {
  type Invariant = () => Boolean

  // See:
  //   http://stackoverflow.com/questions/8178602/using-scala-constructor-to-set-variable-defined-in-trait
  var invariant: Invariant

  // Return whether there exists any execution containing the given external
  // events that triggers the given invariant.
  def test(events: Iterable[ExternalEvent]) : Boolean
}
