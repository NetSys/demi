package akka.dispatch.verification

// Iteratively choose schedules to explore.
// Invoked by WildcardMinimizer.
trait Clusterizer {
  // Approximately how many (possibly wildcarded) schedules are we going to
  // try before we are done?
  def approximateIterations: Int

  // Return the next schedule to explore.
  // If there aren't any more schedules to check, return None.
  //   - violationReproducedLastRun: whether the last schedule we returned
  //   successfully triggered the invariant violation
  //   - ignoredAbsentIds: this set contains
  //   the Unique.id's of all internal events that we tried to include last
  //   time getNextTrace was invoked, but which did not appear in the
  //   execution. These are "freebies" which can be removed without our trying
  //   to remove them, whenever violationReproducedLastRun is true.
  def getNextTrace(violationReproducedLastRun: Boolean, ignoredAbsentIds: Set[Int])
        : Option[EventTrace]
}
