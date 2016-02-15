package akka.dispatch.verification

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge


// A constructor parameter, passed in to all Scheduler objects.
case class SchedulerConfig(
  messageFingerprinter      : FingerprintFactory=null,
  enableFailureDetector     : Boolean=false,
  enableCheckpointing       : Boolean=false,
  shouldShutdownActorSystem : Boolean=true,
  filterKnownAbsents        : Boolean=false,
  invariant_check           : Option[TestOracle.Invariant]=None,
  ignoreTimers              : Boolean=false,
  storeEventTraces          : Boolean=false,
  /**
   * - abortUponDivergence: return "no violation" if we detect that we've
   * diverged from the original schedule, i.e. we arrive at message transitions
   * that have not yet been encountered. The purpose of this option is really
   * just to compare against prior work.
   */
  abortUponDivergence       : Boolean=false,
  // Spark exhibits non-determinism, so instead of detecting specific
  // previously unobserved transitions, just count up all the pending messages at the
  // end of the execution, and see if any were not sent at some point in the
  // original execution.
  abortUponDivergenceLax    : Boolean=false,

  /**
   * - originalDepGraph: DepGraph from the original execution, to detect if we
   * have diverged.
   */
  // TODO(cs): maybe shouldn't store this here.
  originalDepGraph          : Option[Graph[Unique,DiEdge]]=None
)
