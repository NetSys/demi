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
  val ClassOfWaitTimers = classOf[WaitTimers]
  val ClassOfPartition = classOf[Partition]
  val ClassOfUnPartition = classOf[UnPartition]
  val ClassOfContinue = classOf[Continue]
}

// Weights should be between 0 and 1.0.
// At least one weight should be greater than 0.
class FuzzerWeights(
  kill: Double = 0.01,
  send: Double = 0.3,
  wait_quiescence: Double = 0.1,
  wait_timers: Double = 0.3,
  partition: Double = 0.1,
  unpartition: Double = 0.1,
  continue: Double = 0.3) {

  val allWeights = List(kill, send, wait_timers, partition,
                        unpartition, continue)

  var totalMass = allWeights.sum + wait_quiescence

  val eventTypes = List(StableClasses.ClassOfKill, StableClasses.ClassOfSend,
                        StableClasses.ClassOfWaitTimers, StableClasses.ClassOfPartition,
                        StableClasses.ClassOfUnPartition, StableClasses.ClassOfContinue)

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

  val rand = new Random
  val maxContinueSteps = 40

  val nodes = prefix flatMap {
    case Start(_, name) => Some(name)
    case _ => None
  }

  // Note that we don't currently support node recoveries.
  val currentlyAlive = new RandomizedHashSet[String]
  for (node <- nodes) {
    currentlyAlive.insert(node)
  }

  // N.B., only store one direction of the partition
  val currentlyPartitioned = new RandomizedHashSet[(String, String)]
  val currentlyUnpartitioned = new RandomizedHashSet[(String, String)]
  for (i <- (0 to nodes.length-1)) {
    for (j <- (i+1 to nodes.length-1)) {
      currentlyUnpartitioned.insert((nodes(i), nodes(j)))
    }
  }

  // Return None if we have no choice, e.g. if we've killed all nodes, and
  // there's nothing interesting left to do in the execution.
  def generateNextEvent() : Option[ExternalEvent] = {
    val nextEventType = weights.getNextEventType(rand.nextDouble())
    nextEventType match {
      case None => return Some(WaitQuiescence)
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

          case StableClasses.ClassOfWaitTimers =>
            return Some(WaitTimers(1))

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

          case StableClasses.ClassOfContinue =>
            return Some(Continue(rand.nextInt(maxContinueSteps) + 1))
        }
    }
    throw new IllegalStateException("Shouldn't get here")
  }

  def generateFuzzTest() : Seq[ExternalEvent] = {
    val fuzzTest = new ListBuffer[ExternalEvent] ++ prefix
    // Ensure that we don't inject two WaitQuiescense's in a row.
    var justInjectedWaitQuiescence = false

    def okToInject(event: Option[ExternalEvent]) : Boolean = {
      event match {
        case Some(WaitQuiescence) => return !justInjectedWaitQuiescence
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
            case WaitQuiescence =>
              justInjectedWaitQuiescence = true
            case _ => None
              justInjectedWaitQuiescence = false
          }
          fuzzTest += event
        case None => return fuzzTest
      }
    }
    return fuzzTest
  }
}
