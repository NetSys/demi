package akka.dispatch.verification

import akka.actor.FSM.Timer
import akka.actor.{ActorRef, Props, ActorSystem}
import akka.serialization._

import scala.sys.process._
import scala.sys.process.BasicIO
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue

import scala.collection.JavaConversions._

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

import java.io._
import java.nio._
import scala.io._

// Note: in general, try to avoid anonymous functions when writing
// Runner.scala files. This makes deseralization of closures brittle.

trait MessageSerializer {
  def serialize(msg: Any): ByteBuffer
}

class BasicMessageSerializer extends MessageSerializer {
  def serialize(msg: Any): ByteBuffer = {
    return JavaSerialization.serialize(msg)
  }
}

trait MessageDeserializer {
  def deserialize(buf: ByteBuffer): Any
}

class BasicMessageDeserializer(loader:ClassLoader=ClassLoader.getSystemClassLoader())
      extends MessageDeserializer {
  def deserialize(buf: ByteBuffer): Any = {
    return JavaSerialization.deserialize[Any](buf, loader=loader)
  }
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
  val idGenerator = "/idGenerator.bin"
  val actors = "/actors.bin"
  val event_trace = "/event_trace.bin"
  val original_externals = "/original_externals.bin"
  val violation = "/violation.bin"
  val mcs = "/mcs.bin"
  val stats = "/minimization_stats.json"
  val depGraphEdges = "/depGraphEdges.bin"
  val depGraphNodes = "/depGraphNodes.bin"
  // trace of Unique(MsgEvent)s
  val initialTrace = "/initialTrace.bin"
  // initialTrace minus all events that were concurrent with the violation.
  val filteredTrace = "/filteredTrace.bin"
  // trace that have had internal deliveries minimized.
  val minimizedInternalTrace = "/minimizedInternalTrace.bin"
  // trace that was manipulated by hand
  val manualTrace = "/manualTrace.bin"

  def top_level_prefix(): String = {
    // TODO(cs): use a string builder
    var prefix = "."

    def areWeThereYet(): Boolean = {
      return (new java.io.File(prefix)).listFiles().exists(_.getName == "interposition")
    }

    while (!areWeThereYet) {
      prefix = prefix + "/.."
    }
    return prefix + "/"
  }

  def create_experiment_dir(experiment_name: String, add_timestamp:Boolean=true) : String = {
    // Create experiment dir.
    var output_dir = ""
    val errToDevNull = BasicIO(false, (out) => output_dir = out, None)
    val basename = ("basename " + experiment_name).!!
    var cmd = top_level_prefix + "./interposition/src/main/python/setup.py"
    if (add_timestamp) {
      cmd = cmd + " -t"
    }
    val proc = (cmd + " -n " + basename).run(errToDevNull)
    // Block until the process exits.
    proc.exitValue
    return output_dir.trim
  }

  def getActorNameProps(trace: EventTrace) : Seq[Tuple2[Props, String]] = {
    return trace.events.flatMap {
      case SpawnEvent(_,props,name,_) => Some((props, name))
      case _ => None
    }.toSet.toSeq
  }

  def recordMinimizationStats(output_dir: String, internalStats: MinimizationStats) {
    // Just continue overwriting the stats file. Stats are designed to be
    // append only.
    val statsJson = internalStats.toJson()
    JavaSerialization.withPrintWriter(output_dir, ExperimentSerializer.stats) { pw =>
      pw.write(statsJson)
    }
  }
}

class ExperimentSerializer(message_fingerprinter: FingerprintFactory, message_serializer: MessageSerializer) {

  def sanitize_trace(trace: Seq[Event]) : Iterable[Event] = {
    return trace.flatMap(e =>
      e match {
        // Careful how we serialize SpawnEvents' ActorRefs
        case SpawnEvent(parent, props, name, actor) =>
          // For now, nobody uses the ActorRef field of SpawnEvents, so just
          // put deadLetters.
          Some(SerializedSpawnEvent(parent, props, name, "deadLetters"))
        // Can't serialize Timer objects
        case UniqueMsgSend(MsgSend(snd, rcv, Timer(name, nestedMsg, repeat, _)), id) =>
          None
        case UniqueMsgEvent(MsgEvent(snd, rcv, Timer(name, nestedMsg, repeat, generation)), id) =>
          Some(UniqueTimerDelivery(TimerDelivery(snd, rcv, TimerFingerprint(name,
            message_fingerprinter.fingerprint(nestedMsg), repeat, generation)), id=id))
        // Need to serialize external messages
        case u @ UniqueMsgSend(MsgSend("deadLetters", rcv, msg), id) if EventTypes.isExternal(u) =>
          Some(SerializedUniqueMsgSend(SerializedMsgSend("deadLetters", rcv,
            message_serializer.serialize(msg).array()), id))
        case u @ UniqueMsgEvent(MsgEvent("deadLetters", rcv, msg), id) if EventTypes.isExternal(u) =>
          Some(SerializedUniqueMsgEvent(SerializedMsgEvent("deadLetters", rcv,
            message_serializer.serialize(msg).array()), id))
        // Only need to serialize fingerprints for all other messages
        case UniqueMsgSend(MsgSend(snd, rcv, msg), id) =>
          Some(UniqueMsgSend(MsgSend(snd, rcv,
            message_fingerprinter.fingerprint(msg)), id))
        case UniqueMsgEvent(MsgEvent(snd, rcv, msg), id) =>
          Some(UniqueMsgEvent(MsgEvent(snd, rcv,
            message_fingerprinter.fingerprint(msg)), id))
        case event => Some(event)
      }
    )
  }

  def record_experiment(experiment_name: String, trace: EventTrace,
                        violation:ViolationFingerprint=NoViolation,
                        depGraph: Option[Graph[Unique, DiEdge]]=None,
                        initialTrace: Option[Queue[Unique]]=None,
                        filteredTrace: Option[Queue[Unique]]=None) : String = {
    val output_dir = ExperimentSerializer.create_experiment_dir(experiment_name)
    // We store the actor's names and props separately (reduntantly), so that
    // we can properly deserialize ActorRefs later. (When deserializing
    // ActorRefs, we need access to an ActorSystem with all the actors already booted,
    // so that akka can do some magic to resolve the ActorRefs from the old Actorsystem to the
    // corresponding actors in the new ActorSystem. We therefore first boot the
    // system with all these actors created, and then deserialize the rest of the
    // events.)
    record_experiment_known_dir(output_dir, trace, violation,
                                depGraph=depGraph, initialTrace=initialTrace,
                                filteredTrace=filteredTrace)
    return output_dir
  }

  def record_experiment_known_dir(output_dir: String, trace: EventTrace,
                                  violation: ViolationFingerprint,
                                  depGraph: Option[Graph[Unique, DiEdge]]=None,
                                  initialTrace: Option[Queue[Unique]]=None,
                                  filteredTrace: Option[Queue[Unique]]=None) {
    JavaSerialization.writeToFile(output_dir + ExperimentSerializer.idGenerator,
      JavaSerialization.serialize(IDGenerator.uniqueId.get()))

    val actorPropNamePairs = ExperimentSerializer.getActorNameProps(trace)
    val actorNameBuf = JavaSerialization.serialize(actorPropNamePairs.toArray)
    JavaSerialization.writeToFile(output_dir + ExperimentSerializer.actors,
                                  actorNameBuf)

    // Now serialize the events, making sure to nest the serialization of
    // application messages, and making sure to deal with (non-serializable)
    // timers correctly.
    val sanitized = sanitize_trace(trace.events)
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
    if (trace.original_externals != null) {
      val externalsAsArray : Array[ExternalEvent] = trace.original_externals.toArray
      val externalBuf = JavaSerialization.serialize(externalsAsArray)
      JavaSerialization.writeToFile(output_dir + ExperimentSerializer.original_externals,
                                    externalBuf)
    }

    depGraph match {
      case Some(graph) =>
        // We serialize edges and nodes separately, to avoid StackOverFlow.
        val nodes = graph.nodes
        val nodesArray = new Array[Unique](nodes.size)
        nodes.zipWithIndex.foreach {
          case (e,i) =>
            nodesArray(i) = e
        }
        val nodesBuf = JavaSerialization.serialize(nodesArray)
        JavaSerialization.writeToFile(
          output_dir + ExperimentSerializer.depGraphNodes, nodesBuf)

        val edges = graph.edges
        // Tuples of (src id, dst id)
        val edgeArray = new Array[Tuple2[Int,Int]](edges.size)
        edges.zipWithIndex.foreach {
          case (e,i) =>
            edgeArray(i) = ((e._1.id, e._2.id))
        }
        val edgesBuf = JavaSerialization.serialize(edgeArray)
        JavaSerialization.writeToFile(
          output_dir + ExperimentSerializer.depGraphEdges, edgesBuf)
      case None =>
        None
    }

    for ((dporTrace, outputFile) <- Seq(
         (initialTrace, ExperimentSerializer.initialTrace),
         (filteredTrace, ExperimentSerializer.filteredTrace))) {
      dporTrace match {
        case Some(t) =>
          val tAsArray : Array[Unique] = t.toArray
          val traceBuf = JavaSerialization.serialize(tAsArray)
          JavaSerialization.writeToFile(output_dir + outputFile, traceBuf)
        case None =>
          None
      }
    }
  }

  // shrunk: whether the external events have been shrunk
  // (RunnerUtils.shrinkSendContents)
  // Return: the MCS dir
  def serializeMCS(old_experiment_dir: String, mcs: Seq[ExternalEvent],
                   stats: MinimizationStats,
                   mcs_execution: Option[EventTrace],
                   violation: ViolationFingerprint,
                   shrunk: Boolean) : String = {
    val shrunk_str = if (shrunk) "_shrunk" else ""
    val new_experiment_dir = old_experiment_dir + "_" +
        stats.minimization_strategy + "_" + stats.test_oracle + shrunk_str
    ExperimentSerializer.create_experiment_dir(new_experiment_dir, add_timestamp=false)

    recordMinimizedExternals(new_experiment_dir, mcs)

    recordMinimizationStats(new_experiment_dir, stats)

    mcs_execution match {
      case Some(event_trace) =>
        record_experiment_known_dir(new_experiment_dir, event_trace, violation)
      case None => None
    }

    // Overwrite actors.bin, to make sure we include all actors, not just
    // those left in the MCS.
    // TODO(cs): figure out how to do this properly in scala
    ("cp " + old_experiment_dir + ExperimentSerializer.actors + " " + new_experiment_dir).!

    return new_experiment_dir
  }

  def recordMinimizedInternals(output_dir: String,
        internalStats: MinimizationStats, minimized: EventTrace) {
    recordMinimizationStats(output_dir, internalStats)
    val sanitized = sanitize_trace(minimized.events)
    val asArray : Array[Event] = sanitized.toArray
    val sanitizedBuf = JavaSerialization.serialize(asArray)
    JavaSerialization.writeToFile(output_dir + ExperimentSerializer.minimizedInternalTrace,
                                  sanitizedBuf)
  }

  def recordMinimizedExternals(output_dir: String, mcs: Seq[ExternalEvent]) {
    val mcsBuf = JavaSerialization.serialize(mcs.toArray)
    JavaSerialization.writeToFile(output_dir + ExperimentSerializer.mcs,
                                  mcsBuf)
  }

  def recordMinimizationStats(output_dir: String, internalStats: MinimizationStats) {
    // Backwards compat
    ExperimentSerializer.recordMinimizationStats(output_dir, internalStats)
  }

  def recordHandCraftedTrace(output_dir: String, minimized: EventTrace) {
    val sanitized = sanitize_trace(minimized.events)
    val asArray : Array[Event] = sanitized.toArray
    val sanitizedBuf = JavaSerialization.serialize(asArray)
    JavaSerialization.writeToFile(output_dir + ExperimentSerializer.manualTrace,
                                  sanitizedBuf)
  }
}

class ExperimentDeserializer(results_dir: String, loader:ClassLoader=ClassLoader.getSystemClassLoader()) {
  readIfFileExists[Int](results_dir + ExperimentSerializer.idGenerator) match {
    case Some(int) => IDGenerator.uniqueId.set(int)
    case None => None
  }

  def get_actors() : Seq[Tuple2[Props, String]] = {
    val buf = JavaSerialization.readFromFile(results_dir +
      ExperimentSerializer.actors)
    return JavaSerialization.deserialize[Array[Tuple2[Props, String]]](buf, loader=loader)
  }

  def get_violation(message_deserializer: MessageDeserializer): ViolationFingerprint = {
    val buf = JavaSerialization.readFromFile(results_dir +
      ExperimentSerializer.violation)
    return message_deserializer.deserialize(buf).asInstanceOf[ViolationFingerprint]
  }

  def get_mcs(): Seq[ExternalEvent] = {
    val buf = JavaSerialization.readFromFile(results_dir +
      ExperimentSerializer.mcs)
    return JavaSerialization.deserialize[Array[ExternalEvent]](buf, loader=loader)
  }

  private[this] def readIfFileExists[T](file: String): Option[T] = {
   if (new java.io.File(file).exists) {
      val buf = JavaSerialization.readFromFile(file)
      val result = JavaSerialization.deserialize[T](buf, loader=loader)
      return Some(result)
    }
    return None
  }

  def get_dep_graph(): Option[Graph[Unique, DiEdge]] = {
    val nodesOpt = readIfFileExists[Array[Unique]](
      results_dir + ExperimentSerializer.depGraphNodes)
    nodesOpt match {
      case Some(nodes) =>
        val id2node = new HashMap[Int,Unique]
        val graph = Graph[Unique,DiEdge]()
        nodes.foreach {
          case u =>
            id2node(u.id) = u
            graph.add(u)
        }
        val edges = readIfFileExists[Array[Tuple2[Int,Int]]](
          results_dir + ExperimentSerializer.depGraphEdges).get
        edges.foreach {
          case (s,d) =>
            graph.addEdge(id2node(s), id2node(d))(DiEdge)
        }
        return Some(graph)
      case None => return None
    }
  }

  def get_initial_trace(): Option[Queue[Unique]] = {
    val arrayOpt = readIfFileExists[Array[Unique]](results_dir + ExperimentSerializer.initialTrace)
    arrayOpt match {
      case Some(array) =>
        return Some(new Queue[Unique] ++ array)
      case None => return None
    }
  }

  def get_filtered_initial_trace(): Option[Queue[Unique]] = {
    val arrayOpt = readIfFileExists[Array[Unique]](results_dir + ExperimentSerializer.filteredTrace)
    arrayOpt match {
      case Some(array) =>
        return Some(new Queue[Unique] ++ array)
      case None => return None
    }
  }

  def get_events(message_deserializer: MessageDeserializer,
                 actorSystem: ActorSystem,
                 traceFile:String=ExperimentSerializer.event_trace) : EventTrace = {
    val buf = JavaSerialization.readFromFile(results_dir + traceFile)
    val events = JavaSerialization.deserialize[Array[Event]](buf, loader=loader).map(e =>
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
    val originalExternals = JavaSerialization.deserialize[Array[ExternalEvent]](originalExternalBuf, loader=loader)

    val queue = new SynchronizedQueue[Event]
    queue ++= events
    return new EventTrace(queue, originalExternals)
  }

  def get_stats(): MinimizationStats = {
    val source = scala.io.Source.fromFile(results_dir + ExperimentSerializer.stats)
    val lines = try source.mkString finally source.close()
    return MinimizationStats.fromJson(lines)
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

  def deserialize[T](b: ByteBuffer, loader:ClassLoader=ClassLoader.getSystemClassLoader()) : T = {
    val bis = new ByteArrayInputStream(b.array())
    var in: ObjectInput = null
    try {
      in = new ObjectInputStream(bis) {
        override def resolveClass(desc: ObjectStreamClass) =
          Class.forName(desc.getName, false, loader)
      }
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

  def withPrintWriter(dir:String, name:String)(f: (PrintWriter) => Any) {
    val file = new File(dir, name)
    val writer = new FileWriter(file)
    val printWriter = new PrintWriter(writer)
    try {
      f(printWriter)
    }
    finally {
      printWriter.close()
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
