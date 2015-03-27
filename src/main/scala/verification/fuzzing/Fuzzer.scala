package akka.dispatch.verification

import scala.collection.mutable.ListBuffer
import scala.util.Random


// Needs to be implemented by the application.
trait MessageGenerator {
  def generateMessage(aliveActors: RandomizedHashSet[String]) : Send
}

object StableClasses {
  // Deal with type erasue...
  // See:
  //   http://stackoverflow.com/questions/7157143/how-can-i-match-classes-in-a-scala-match-statement
  val ClassOfKill = classOf[Kill]
  val ClassOfSend = classOf[Send]
  val ClassOfPartition = classOf[Partition]
  val ClassOfUnPartition = classOf[UnPartition]
}

// Weights should be between 0 and 1.0.
// At least one weight should be greater than 0.
case class FuzzerWeights(
  kill: Double = 0.01,
  send: Double = 0.3,
  wait_quiescence: Double = 0.1,
  partition: Double = 0.1,
  unpartition: Double = 0.1) {

  val allWeights = List(kill, send, partition, unpartition)

  val totalMass = allWeights.sum + wait_quiescence

  val eventTypes = List(StableClasses.ClassOfKill, StableClasses.ClassOfSend,
                        StableClasses.ClassOfPartition, StableClasses.ClassOfUnPartition)

  // Takes a number between [0, 1.0]
  // Returns the class of an ExternalEvent, or None if the chosen class type
  // is WaitQuiescence.
  // We treat WaitQuiescence specially, since it's a case object instead of
  // a case class, and I don't really file like wrestling with the scala type system
  // right now.
  def getNextEventType(r: Double) : Option[Class[_ <: ExternalEvent]] = {
    // First scale r to [0, totalMass]
    val scaled = r * totalMass
    // Now, a simple O(n) algorithm to decide which event type to return.
    // N=7, so whatever.
    var currentWeight = 0.0
    for ((weight, index) <- allWeights.zipWithIndex) {
      currentWeight = currentWeight + weight
      if (scaled < currentWeight) {
        return Some(eventTypes(index))
      }
    }
    return None
  }
}

// prefix must at least contain start events
class Fuzzer(num_events: Integer,
             weights: FuzzerWeights,
             message_gen: MessageGenerator,
             prefix: Seq[ExternalEvent]) {

  var seed = System.currentTimeMillis
  var rand = new Random(seed)

  val nodes = prefix flatMap {
    case Start(_, name) => Some(name)
    case _ => None
  }

  // Note that we don't currently support node recoveries.
  var currentlyAlive = new RandomizedHashSet[String]

  // N.B., only store one direction of the partition
  var currentlyPartitioned = new RandomizedHashSet[(String, String)]
  var currentlyUnpartitioned = new RandomizedHashSet[(String, String)]

  // Return None if we have no choice, e.g. if we've killed all nodes, and
  // there's nothing interesting left to do in the execution.
  def generateNextEvent() : Option[ExternalEvent] = {
    val nextEventType = weights.getNextEventType(rand.nextDouble())
    nextEventType match {
      case None => return Some(WaitQuiescence())
      case Some(cls) =>
        cls match {
          case StableClasses.ClassOfKill =>
            if (currentlyAlive.isEmpty)
              return None
            val nextVictim = currentlyAlive.removeRandomElement()
            return Some(Kill(nextVictim))

          case StableClasses.ClassOfSend =>
            val send = message_gen.generateMessage(currentlyAlive)
            return Some(send)

          case StableClasses.ClassOfPartition =>
            if (currentlyUnpartitioned.isEmpty) {
              // Try again...
              return generateNextEvent()
            }
            val pair = currentlyUnpartitioned.removeRandomElement()
            currentlyPartitioned.insert(pair)
            return Some(Partition(pair._1, pair._2))

          case StableClasses.ClassOfUnPartition =>
            if (currentlyPartitioned.isEmpty) {
              // Try again...
              return generateNextEvent()
            }
            val pair = currentlyPartitioned.removeRandomElement()
            currentlyUnpartitioned.insert(pair)
            return Some(UnPartition(pair._1, pair._2))
        }
    }
    throw new IllegalStateException("Shouldn't get here")
  }

  def generateFuzzTest() : Seq[ExternalEvent] = {
    reset()
    val fuzzTest = new ListBuffer[ExternalEvent] ++ prefix

    def validateFuzzTest(_fuzzTest: Seq[ExternalEvent]) {
      for (i <- (0 until _fuzzTest.length - 1)) {
        if (_fuzzTest(i).getClass() == classOf[WaitQuiescence] &&
            _fuzzTest(i+1).getClass() == classOf[WaitQuiescence]) {
          throw new AssertionError(i + " " + (i+1) + ". Seed was: " + seed)
        }
      }
    }

    // Ensure that we don't inject two WaitQuiescense's in a row.
    var justInjectedWaitQuiescence = if (fuzzTest.length > 0)
        fuzzTest(fuzzTest.length-1).getClass == classOf[WaitQuiescence]
        else false

    def okToInject(event: Option[ExternalEvent]) : Boolean = {
      event match {
        case Some(WaitQuiescence()) =>
          return !justInjectedWaitQuiescence
        case _ => return true
      }
    }

    for (_ <- (1 to num_events)) {
      var nextEvent = generateNextEvent()
      while (!okToInject(nextEvent)) {
        nextEvent = generateNextEvent()
      }
      nextEvent match {
        case Some(event) =>
          event match {
            case WaitQuiescence() =>
              justInjectedWaitQuiescence = true
            case _ =>
              justInjectedWaitQuiescence = false
          }
          fuzzTest += event
        case None =>
          validateFuzzTest(fuzzTest)
          return fuzzTest
      }
    }

    validateFuzzTest(fuzzTest)
    return fuzzTest
  }

  def reset() {
    seed = System.currentTimeMillis
    rand = new Random(seed)

    currentlyAlive = new RandomizedHashSet[String]
    for (node <- nodes) {
      currentlyAlive.insert(node)
    }

    currentlyPartitioned = new RandomizedHashSet[(String, String)]
    currentlyUnpartitioned = new RandomizedHashSet[(String, String)]
    for (i <- (0 to nodes.length-1)) {
      for (j <- (i+1 to nodes.length-1)) {
        currentlyUnpartitioned.insert((nodes(i), nodes(j)))
      }
    }
  }
}
