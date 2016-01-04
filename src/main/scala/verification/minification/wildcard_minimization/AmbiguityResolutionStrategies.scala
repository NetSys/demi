package akka.dispatch.verification

import scala.collection.mutable.HashSet

// When there are multiple pending messages between a given src,dst pair,
// choose how the WildCardMatch's message selector should be applied to the
// pending messages.
trait AmbiguityResolutionStrategy {
  type MessageSelector = (Any) => Boolean

  // Return the index of the selected message, if any.
  def resolve(msgSelector: MessageSelector, pending: Seq[Any],
              backtrackSetter: (Int) => Unit) : Option[Int]
}

// TODO(cs): SrcDstFIFOOnly technically isn't enough to ensure FIFO removal --
// currently ClockClusterizer can violate the FIFO scheduling discipline.
class SrcDstFIFOOnly extends AmbiguityResolutionStrategy {
  // If the first pending message doesn't match, give up.
  // For Timers though, allow any of them to matched.
  def resolve(msgSelector: MessageSelector, pending: Seq[Any],
              backtrackSetter: (Int) => Unit) : Option[Int] = {
    pending.headOption match {
      case Some(msg) =>
        if (msgSelector(msg))
          Some(0)
        else
          None
      case None =>
        None
    }
  }
}

// TODO(cs): implement SrcDstFIFO strategy that adds backtrack point for
// playing unexpected events at the front of the FIFO queue whenever the WildCard
// doesn't match the head (but a pending event later does?).
// Key constraint: need to make sure that we're always making progress, i.e.
// what we're exploring would be shorter if it pans out.

// For UDP bugs: set a backtrack point every time there are multiple pending
// messages of the same type, and have DPORwHeuristics go back and explore
// those.
class BackTrackStrategy(messageFingerprinter: FingerprintFactory) extends AmbiguityResolutionStrategy {
  def resolve(msgSelector: MessageSelector, pending: Seq[Any],
              backtrackSetter: (Int) => Unit) : Option[Int] = {
    val matching = pending.zipWithIndex.filter(
      { case (msg,i) => msgSelector(msg) })

    if (!matching.isEmpty) {
      // Set backtrack points, if any, for any messages that are of the same
      // type, but not exactly the same as eachother.
      val alreadyTried = new HashSet[MessageFingerprint]
      alreadyTried += messageFingerprinter.fingerprint(matching.head._1)
      // Heuristic: reverse the remaining backtrack points, on the assumption
      // that the last one is more fruitful than the middle ones
      matching.tail.reverse.filter {
        case (msg,i) =>
          val fingerprinted = messageFingerprinter.fingerprint(msg)
          if (!(alreadyTried contains fingerprinted)) {
            alreadyTried += fingerprinted
            true
          } else {
            false
          }
      }.foreach {
        case (msg,i) => backtrackSetter(i)
      }

      return matching.headOption.map(t => t._2)
    }

    return None
  }
}

// Same as above, but only focus on the first and last matching message.
class FirstAndLastBacktrack(messageFingerprinter: FingerprintFactory) extends AmbiguityResolutionStrategy {
  def resolve(msgSelector: MessageSelector, pending: Seq[Any],
              backtrackSetter: (Int) => Unit) : Option[Int] = {
    val matching = pending.zipWithIndex.filter(
      { case (msg,i) => msgSelector(msg) })

    if (!matching.isEmpty) {
      // Set backtrack points, if any, for any messages that are of the same
      // type, but not exactly the same as eachother.
      val alreadyTried = new HashSet[MessageFingerprint]
      alreadyTried += messageFingerprinter.fingerprint(matching.head._1)
      matching.tail.reverse.find {
        case (msg,i) =>
          val fingerprinted = messageFingerprinter.fingerprint(msg)
          if (!(alreadyTried contains fingerprinted)) {
            alreadyTried += fingerprinted
            true
          } else {
            false
          }
      }.foreach {
        case (msg,i) => backtrackSetter(i)
      }

      return matching.headOption.map(t => t._2)
    }

    return None
  }
}

class LastOnlyStrategy extends AmbiguityResolutionStrategy {
  def resolve(msgSelector: MessageSelector, pending: Seq[Any],
              backtrackSetter: (Int) => Unit) : Option[Int] = {
    val matching = pending.zipWithIndex.filter(
      { case (msg,i) => msgSelector(msg) })

    return matching.lastOption.map(t => t._2)
  }
}
