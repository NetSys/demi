package akka.dispatch.verification

trait MessageFingerprint {
  def hashCode: Int
  def equals(other: Any) : Boolean
}

trait MessageFingerprinter {
  // Account for the possibility of serialized messages, that have been
  // serialized as MessageFingerprints. Subclasses should probably call this.
  def fingerprint(msg: Any) : MessageFingerprint = {
    if (msg.isInstanceOf[MessageFingerprint]) {
      return msg.asInstanceOf[MessageFingerprint]
    }
    return null
  }
}

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
  override def fingerprint(msg: Any) : MessageFingerprint = {
    val superResult = super.fingerprint(msg)
    if (superResult != null) {
      return superResult
    }
    return BasicFingerprint(msg.toString)
  }
}
