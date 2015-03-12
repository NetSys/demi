package akka.dispatch.verification

import akka.actor.FSM
import akka.actor.FSM.Timer

import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer

trait MessageFingerprint {
  def hashCode: Int
  def equals(other: Any) : Boolean
}

trait MessageFingerprinter {
  // Return Some(fingerprint) if this fingerprinter knows anything about
  // msg, otherwise return None and fallback to another fingerprinter
  def fingerprint(msg: Any) : Option[MessageFingerprint] = {
    // Account for the possibility of serialized messages, that have been
    // serialized as MessageFingerprints. Subclasses should probably call this.
    if (msg.isInstanceOf[MessageFingerprint]) {
      return Some(msg.asInstanceOf[MessageFingerprint])
    }
    return None
  }
}

// A simple fingerprint template for user-defined fingerprinters. Should
// usually not be instantiated directly; invoke BasicFingerprint.fromMessage()
case class BasicFingerprint(val str: String) extends MessageFingerprint

// Static constructor for BasicFingeprrint
object BasicFingerprint {
  val systemRegex = ".*(new-system-\\d+).*".r

  def fromMessage(msg: Any): BasicFingerprint = {
    // Lazy person's approach: for any message that we didn't explicitly match on,
    // their toString might contain a string referring to the ActorSystem, which
    // changes across runs. Run a simple regex over it to catch that.
    val str = msg.toString match {
      case systemRegex(system) => msg.toString.replace(system, "")
      case _ => msg.toString
    }
    return BasicFingerprint(str)
  }
}

// Singleton for TimeoutMaker
case object TimeoutMarkerFingerprint extends MessageFingerprint

class BaseFingerprinter(parent: FingerprintFactory) extends MessageFingerprinter {
  override def fingerprint(msg: Any) : Option[MessageFingerprint] = {
    val alreadyFingerprint = super.fingerprint(msg)
    if (alreadyFingerprint != None) {
      return alreadyFingerprint
    }
    if (ClassTag(msg.getClass).toString == "akka.actor.FSM$TimeoutMarker") {
      return Some(TimeoutMarkerFingerprint)
    }
    msg match {
      case Timer(name, message, repeat, _) =>
        return Some(TimerFingerprint(name, parent.fingerprint(message), repeat))
      case _ =>
        // BaseFingerprinter is the most general fingerprinter, so it always returns Some.
        return Some(BasicFingerprint.fromMessage(msg))
    }
  }
}

class FingerprintFactory {
  val fingerprinters = new ListBuffer[MessageFingerprinter] ++
      List(new BaseFingerprinter(this))

  def registerFingerprinter(fingerprinter: MessageFingerprinter) {
    // Always prepend. BaseFingerprinter should be last
    fingerprinters.+=:(fingerprinter)
  }

  def fingerprint(msg: Any) : MessageFingerprint = {
    for (fingerprinter <- fingerprinters) {
      val fingerprint = fingerprinter.fingerprint(msg)
      fingerprint match {
        case None => None
        case Some(f) => return f
      }
    }
    throw new IllegalStateException("Should have matched")
  }
}
