package akka.dispatch.verification

import akka.actor.FSM.Timer
import akka.actor.{ActorRef, Props, ActorSystem}
import akka.serialization._

import scala.sys.process._
import scala.sys.process.BasicIO
import scala.collection.mutable.Queue

import java.io._
import java.nio._
import scala.io._

trait MessageSerializer {
  def serialize(msg: Any): ByteBuffer
}

trait MessageDeserializer {
  def deserialize(buf: ByteBuffer): Any
}

// classLoaders for the application's messages are not in scope within this file,
// which makes it impossible for us to deserialize the application's messages. We therefore store
// the serialized messages as a nested Array[Byte] instance variable, and then later have the application
// deserialize those Array[Byte]s for us.
case class SerializedMsgSend(sender: String, receiver: String, msgBuf: Array[Byte]) extends Event
case class SerializedMsgEvent(sender: String, receiver: String, msgBuf: Array[Byte]) extends Event
case class SerializedUniqueMsgSend(m: SerializedMsgSend, id: Int) extends Event
case class SerializedUniqueMsgEvent(m: SerializedMsgEvent, id: Int) extends Event
case class SerializedSpawnEvent(parent: String, props: Props, name: String, actor: String) extends Event

object ExperimentSerializer {
  val actors = "/actors.bin"
  val event_trace = "/event_trace.bin"
  val original_externals = "/original_externals.bin"
  val violation = "/violation.bin"
  val mcs = "/mcs.bin"
  val stats = "/minimization_stats.json"

  def create_experiment_dir(experiment_name: String) : String = {
    // Create experiment dir.
    var output_dir = ""
    val errToDevNull = BasicIO(false, (out) => output_dir = out, None)
    val proc = (f"./interposition/src/main/python/setup.py -t -n " + experiment_name).run(errToDevNull)
    // Block until the process exits.
    proc.exitValue
    return output_dir.trim
  }
}

class ExperimentSerializer(message_fingerprinter: MessageFingerprinter, message_serializer: MessageSerializer) {

  def record_experiment(experiment_name: String, trace: EventTrace,
                        violation: ViolationFingerprint) : String = {
    val output_dir = ExperimentSerializer.create_experiment_dir(experiment_name)
    // We store the actor's names and props separately (reduntantly), so that
    // we can properly deserialize ActorRefs later. (When deserializing
    // ActorRefs, we need access to an ActorSystem with all the actors already booted,
    // so that akka can do some magic to resolve the ActorRefs from the old Actorsystem to the
    // corresponding actors in the new ActorSystem. We therefore first boot the
    // system with all these actors created, and then deserialize the rest of the
    // events.)
    val actorPropNamePairs = trace.events.flatMap {
      case SpawnEvent(_,props,name,_) => Some((props, name))
      case _ => None
    }
    val actorNameBuf = JavaSerialization.serialize(actorPropNamePairs.toSet.toArray)
    JavaSerialization.writeToFile(output_dir + ExperimentSerializer.actors,
                                  actorNameBuf)

    // Now serialize the events, making sure to nest the serialization of
    // application messages, and making sure to deal with (non-serializable)
    // timers correctly.
    val sanitized = trace.events.map(e =>
      e match {
        // Careful how we serialize SpawnEvents' ActorRefs
        case SpawnEvent(parent, props, name, actor) =>
          // For now, nobody uses the ActorRef field of SpawnEvents, so just
          // put deadLetters.
          SerializedSpawnEvent(parent, props, name, "deadLetters")
        // Can't serialize Timer objects
        case UniqueMsgSend(MsgSend(snd, rcv, Timer(name, nestedMsg, repeat, generation)), id) =>
          TimerSend(TimerFingerprint(name, snd, rcv,
            message_fingerprinter.fingerprint(nestedMsg), repeat, generation))
        case UniqueMsgEvent(MsgEvent(snd, rcv, Timer(name, nestedMsg, repeat, generation)), id) =>
          TimerDelivery(TimerFingerprint(name, snd, rcv,
            message_fingerprinter.fingerprint(nestedMsg), repeat, generation))
        // Need to serialize external messages
        case UniqueMsgSend(MsgSend("deadLetters", rcv, msg), id) =>
          SerializedUniqueMsgSend(SerializedMsgSend("deadLetters", rcv,
            message_serializer.serialize(msg).array()), id)
        case UniqueMsgEvent(MsgEvent("deadLetters", rcv, msg), id) =>
          SerializedUniqueMsgEvent(SerializedMsgEvent("deadLetters", rcv,
            message_serializer.serialize(msg).array()), id)
        // Only need to serialize fingerprints for all other messages
        case UniqueMsgSend(MsgSend(snd, rcv, msg), id) =>
          UniqueMsgSend(MsgSend(snd, rcv,
            message_fingerprinter.fingerprint(msg)), id)
        case UniqueMsgEvent(MsgEvent(snd, rcv, msg), id) =>
          UniqueMsgEvent(MsgEvent(snd, rcv,
            message_fingerprinter.fingerprint(msg)), id)
        case event => event
      }
    )
    // Use a data structure that won't cause stackoverflow on
    // serialization. See:
    // http://stackoverflow.com/questions/25147565/serializing-java-object-without-stackoverflowerror
    val asArray : Array[Event] = sanitized.toArray

    val sanitizedBuf = JavaSerialization.serialize(asArray)
    JavaSerialization.writeToFile(output_dir + ExperimentSerializer.event_trace,
                                  sanitizedBuf)

    // Serialize the violation
    val violationBuf = JavaSerialization.serialize(violation)
    JavaSerialization.writeToFile(output_dir + ExperimentSerializer.violation,
                                  violationBuf)

    // Serialize the external events.
    val externalBuf = message_serializer.serialize(trace.original_externals)
    JavaSerialization.writeToFile(output_dir + ExperimentSerializer.original_externals,
                                  externalBuf)

    return output_dir
  }
}

class ExperimentDeserializer(results_dir: String) {
  def get_actors() : Seq[Tuple2[Props, String]] = {
    val buf = JavaSerialization.readFromFile(results_dir +
      ExperimentSerializer.actors)
    return JavaSerialization.deserialize[Array[Tuple2[Props, String]]](buf)
  }

  def get_violation(message_deserializer: MessageDeserializer): ViolationFingerprint = {
    val buf = JavaSerialization.readFromFile(results_dir +
      ExperimentSerializer.violation)
    return message_deserializer.deserialize(buf).asInstanceOf[ViolationFingerprint]
  }

  def get_events(message_deserializer: MessageDeserializer, actorSystem: ActorSystem) : EventTrace = {
    val buf = JavaSerialization.readFromFile(results_dir +
      ExperimentSerializer.event_trace)
    val events = JavaSerialization.deserialize[Array[Event]](buf).map(e =>
      e match {
        case SerializedSpawnEvent(parent, props, name, actor) =>
          // For now, nobody uses the ActorRef field of SpawnEvents, so just
          // put deadLetters.
          SpawnEvent(parent, props, name, actorSystem.deadLetters)
        case SerializedUniqueMsgSend(SerializedMsgSend(snd, rcv, msgBuf), id) =>
          UniqueMsgSend(MsgSend(snd, rcv, message_deserializer.deserialize((ByteBuffer.wrap(msgBuf)))), id)
        case SerializedUniqueMsgEvent(SerializedMsgEvent(snd, rcv, msgBuf), id) =>
          UniqueMsgEvent(MsgEvent(snd, rcv, message_deserializer.deserialize((ByteBuffer.wrap(msgBuf)))), id)
        case event =>
          event
      }
    )

    val originalExternalBuf = JavaSerialization.readFromFile(results_dir +
      ExperimentSerializer.original_externals)
    // N.B. sbt does some strange things with the class path, and sometimes
    // fails on this line. One way of fixing this: rather than running
    // `sbt run`, invoke `sbt assembly; java -cp /path/to/assembledjar Main`
    val originalExternals = JavaSerialization.deserialize[Seq[ExternalEvent]](originalExternalBuf)

    return new EventTrace(new Queue[Event] ++ events, originalExternals)
  }
}

object JavaSerialization {
  def serialize(o: Any) : ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    var out : ObjectOutput = null
    try {
      out = new ObjectOutputStream(bos)
      out.writeObject(o)
      return ByteBuffer.wrap(bos.toByteArray)
    } finally {
      try {
        if (out != null) {
          out.close()
        }
      } catch {
        case ex: IOException => None
      }
      try {
        bos.close();
      } catch {
        case ex: IOException => None
      }
    }
  }

  def deserialize[T](b: ByteBuffer) : T = {
    val bis = new ByteArrayInputStream(b.array())
    var in: ObjectInput = null
    try {
      in = new ObjectInputStream(bis)
      return in.readObject().asInstanceOf[T]
    } finally {
      try {
        bis.close();
      } catch {
        case ex: IOException => None
      }
      try {
        if (in != null) {
          in.close();
        }
      } catch {
        case ex: IOException => None
      }
    }
  }

  def writeToFile(filename: String, buf: ByteBuffer) {
    val file = new File(filename)
    var fos: FileOutputStream = null
    try {
      fos = new FileOutputStream(file)
      // Writes bytes from the specified byte array to this file output stream
      fos.write(buf.array())
    } finally {
      try {
        if (fos != null) {
           fos.close()
        }
      } catch {
        case ioe: IOException => None
      }
    }
  }

  def writeToFile(filename: String, contents: String) {
    val file = new File(filename)
    var pw : PrintWriter = null
    try {
      val pw = new PrintWriter(file)
      pw.write(contents)
    } finally {
      try {
        if (pw != null) {
          pw.close()
        }
      } catch {
        case ioe: IOException => None
      }
    }
  }

  def readFromFile(filename: String) : ByteBuffer = {
    var fis : FileInputStream = null
    try {
      fis = new FileInputStream(filename)
      val bis = new BufferedInputStream(fis)
      // TODO(cs): slow
      return ByteBuffer.wrap(Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toArray)
    } finally {
      try {
        if (fis != null) {
           fis.close()
        }
      } catch {
        case ioe: IOException => None
      }
    }
  }
}
