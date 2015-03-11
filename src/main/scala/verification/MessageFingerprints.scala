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

// A simple fingerprint template for user-defined fingerprinters
case class BasicFingerprint(str: String) extends MessageFingerprint {
  override def equals(other: Any) : Boolean = {
    other match {
      case BasicFingerprint(otherStr) =>
        return otherStr == str
      case _ =>
        return false
    }
  }
  override def hashCode = str.hashCode
}

// Most general fingerprint, matches last
case class BaseFingerprint(val msg: Any) extends MessageFingerprint {
  val systemRegex = ".*(new-system-\\d+).*".r

  // Lazy person's approach: for any message that we didn't explicitly match on,
  // their toString might contain a string referring to the ActorSystem, which
  // changes across runs. Run a simple regex over it to catch that.
  val str = msg.toString match {
    case systemRegex(system) => msg.toString.replace(system, "")
    case _ => msg.toString
  }

  override def hashCode : Int = {
    msg match {
      // TODO(cs): apply a nested fingerprint on msg?
      case Timer(name, msg, repeat, _) =>
        return name.hashCode * 41 + msg.hashCode * 7 + (if (repeat) 1 else 0)
      case _ =>
        if (ClassTag(msg.getClass).toString == "akka.actor.FSM$TimeoutMarker") {
          return 42
        }
        return str.hashCode
    }
  }

  override def equals(other: Any) : Boolean = {
    return other match {
      case b: BaseFingerprint =>
        // Ugly hack since TimeoutMarker is private in new enough (> 2.0) Akka versions.
        (msg, b.msg) match {
          case (Timer(n1, m1, rep1, _), Timer(n2, m2, rep2, _)) =>
            (n1 == n2) && (m1 == m2) && (rep1 == rep2)
          case (m1, m2) =>
            (ClassTag(m1.getClass).toString, ClassTag(m2.getClass).toString) match {
              case ("akka.actor.FSM$TimeoutMarker", "akka.actor.FSM$TimeoutMarker") => true
              case _ => str.equals(b.str)
            }
        }
      case _ => return false
    }
  }
}

class BaseFingerprinter extends MessageFingerprinter {
  override def fingerprint(msg: Any) : Option[MessageFingerprint] = {
    val alreadyFingerprint = super.fingerprint(msg)
    if (alreadyFingerprint != None) {
      return alreadyFingerprint
    }
    // BaseFingerprinter is the most general fingerprinter, so it always returns Some.
    return Some(BaseFingerprint(msg))
  }
}

class FingerprintFactory {
  val fingerprinters = new ListBuffer[MessageFingerprinter] ++ List(new BaseFingerprinter)

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
