package akka.dispatch.verification

import scala.collection.mutable.Queue
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.Envelope

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

object DepTracker {
  val root = Unique(MsgEvent("null", "null", null), 0)
}

/**
 * For every message we deliver, track which messages become enabled as a
 * result of our delivering that message. This can be used later by DPOR.
 *
 * - noopWaitQuiescence: if true, always attach external events to the root.
 */
// TODO(cs): Should probably factor out the Graph management code from DPOR
// and put it here.
class DepTracker(schedulerConfig: SchedulerConfig,
                 noopWaitQuiescence:Boolean=true) {
  val log = LoggerFactory.getLogger("DepTracker")
  val depGraph = schedulerConfig.originalDepGraph match {
    case Some(g) => g
    case None => Graph[Unique, DiEdge]()
  }
  val initialTrace = new Queue[Unique]

  def getRootEvent() : Unique = {
    require(DepTracker.root != null)
    addGraphNode(DepTracker.root)
    DepTracker.root
  }

  // TODO(cs): probably not necessary? Just do getRootEvent()
  // Special marker for the 0th event
  val currentRoot = depGraph.isEmpty match {
    case true => getRootEvent()
    case false =>
      // Since the graph does reference equality, need to reset _root to the
      // root in the graph.
      def matchesRoot(n: Unique) : Boolean = {
        return n.event == MsgEvent("null", "null", null)
      }
      depGraph.nodes.toList.find(matchesRoot _).map(_.value).getOrElse(
        throw new IllegalArgumentException("No root in initialDepGraph"))
  }

  initialTrace += currentRoot
  // Most recent message we delivered.
  var parentEvent : Unique = currentRoot
  // Most recent quiescence we saw
  var lastQuiescence : Unique = currentRoot

  private[this] def addGraphNode (event: Unique) = {
    require(event != null)
    depGraph.add(event)
  }

  def isUnknown(unique: Unique) = (depGraph find unique) == None

  /**
   * Given a message, figure out if we have already seen
   * it before. We achieve this by consulting the
   * dependency graph.
   *
   * * @param (cell, envelope): Original message context.
   *
   * * @return A unique event.
   */
  def getMessage(snd: String, rcv: String, msg: Any, external:Boolean=false) : Unique = {
    if (external) parentEvent = lastQuiescence
    val fingerprinted = schedulerConfig.messageFingerprinter.fingerprint(msg)
    val msgEvent = new MsgEvent(snd, rcv, fingerprinted)

    // Who cares if the parentEvent is in fact a message, as long as it is a parent.
    val parent = parentEvent

    def matchMessage (event: Event) : Boolean = {
      event match {
        // Second fingerprint isn't strictly necessary, but just be cautious
        // anyway
        case MsgEvent(s,r,m) => (s == snd && r == rcv &&
          schedulerConfig.messageFingerprinter.fingerprint(m) == fingerprinted)
        case e => throw new IllegalStateException("Not a MsgEvent: " + e)
      }
    }

    val inNeighs = depGraph.get(parent).inNeighbors
    inNeighs.find { x => matchMessage(x.value.event) } match {
      case Some(x) =>
        return x.value
      case None =>
        val newMsg = Unique(msgEvent)
        return newMsg
      case _ => throw new Exception("wrong type")
    }
  }

  def addNodeAndEdge(child: Unique) {
    if (isUnknown(child)) {
      depGraph.add(child)
      depGraph.addEdge(child, parentEvent)(DiEdge)
    }
  }

  // External messages
  def reportNewlyEnabledExternal(snd: String, rcv: String, msg: Any): Unique = {
    parentEvent = lastQuiescence
    return reportNewlyEnabled(snd, rcv, msg)
  }

  // Return an id for this message. Must return this same id upon
  // reportNewlyDelivered.
  def reportNewlyEnabled(snd: String, rcv: String, msg: Any): Unique = {
    val child = getMessage(snd, rcv, msg)
    addNodeAndEdge(child)
    return child
  }

  def reportNewlyDelivered(u: Unique) {
    parentEvent = u
    initialTrace += u
  }

  // Report when quiescence has been arrived at as a result of an external
  // WaitQuiescence event.
  def reportQuiescence(w: WaitQuiescence) {
    if (!noopWaitQuiescence) {
      parentEvent = lastQuiescence
      val quiescence = Unique(w, id=w._id)
      println("reportQuiescence: " + quiescence)
      depGraph.add(quiescence)
      depGraph.addEdge(quiescence, parentEvent)(DiEdge)
      lastQuiescence = quiescence
      initialTrace += quiescence
      parentEvent = quiescence
    }
  }

  def reportKill(name: String, allActors: Set[String], id: Int) {
    val unique = Unique(NetworkPartition(Set(name), allActors), id=id)
    depGraph.add(unique)
    initialTrace += unique
  }

  def reportPartition(a: String, b: String, id: Int) {
    val unique = Unique(NetworkPartition(Set(a), Set(b)), id=id)
    depGraph.add(unique)
    initialTrace += unique
  }

  def reportUnPartition(a: String, b: String, id: Int) {
    val unique = Unique(NetworkUnpartition(Set(a), Set(b)), id=id)
    depGraph.add(unique)
    initialTrace += unique
  }

  def getGraph(): Graph[Unique, DiEdge] = depGraph

  def getInitialTrace(): Queue[Unique] = initialTrace
}
