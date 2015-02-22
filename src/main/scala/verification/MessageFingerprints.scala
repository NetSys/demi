package akka.dispatch.verification

trait MessageFingerprint {
  def hashCode: Int
  def equals(other: Any) : Boolean
}

trait MessageFingerprinter {
  def fingerprint(msg: Any) : MessageFingerprint
}

//case class BasicFingerprint(str: String) extends MessageFingerprint

case class BasicFingerprint(str: String) extends MessageFingerprint {
  override def hashCode = str.hashCode
  override def equals(other: Any) : Boolean = {
    other match {
      case BasicFingerprint(otherStr) =>
        return str.equals(otherStr)
      case _ => return false
    }
  }
}

class BasicFingerprinter extends MessageFingerprinter {
  def fingerprint(msg: Any) : MessageFingerprint = {
    return BasicFingerprint(msg.toString)
  }
}
