package akka.dispatch.verification

import scala.collection.mutable.Queue
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}
import akka.dispatch.Envelope

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge


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
class DepTracker(messageFingerprinter: FingerprintFactory,
                 noopWaitQuiescence:Boolean=true) {
  val g = Graph[Unique, DiEdge]()
  val initialTrace = new Queue[Unique]
  // Special marker for the 0th event
  val root = DepTracker.root
  g.add(root)
  initialTrace += root
  // Most recent message we delivered.
  var parent : Unique = root
  // Most recent quiescence we saw
  var lastQuiescence : Unique = root

  // External messages
  def reportNewlyEnabledExternal(snd: String, rcv: String, msg: Any): Unique = {
    parent = lastQuiescence
    return reportNewlyEnabled(snd, rcv, msg)
  }

  // Return an id for this message. Must return this same id upon
  // reportNewlyDelivered.
  def reportNewlyEnabled(snd: String, rcv: String, msg: Any): Unique = {
    val child = Unique(MsgEvent(snd, rcv,
      messageFingerprinter.fingerprint(msg)))
    g.add(child)
    g.addEdge(child, parent)(DiEdge)
    return child
  }

  def reportNewlyDelivered(u: Unique) {
    parent = u
    initialTrace += u
  }

  // Report when quiescence has been arrived at as a result of an external
  // WaitQuiescence event.
  def reportQuiescence(w: WaitQuiescence) {
    if (!noopWaitQuiescence) {
      parent = lastQuiescence
      val quiescence = Unique(w, id=w._id)
      println("reportQuiescence: " + quiescence)
      g.add(quiescence)
      g.addEdge(quiescence, parent)(DiEdge)
      lastQuiescence = quiescence
      initialTrace += quiescence
      parent = quiescence
    }
  }

  def reportKill(name: String, allActors: Set[String], id: Int) {
    val unique = Unique(NetworkPartition(Set(name), allActors), id=id)
    g.add(unique)
    initialTrace += unique
  }

  def reportPartition(a: String, b: String, id: Int) {
    val unique = Unique(NetworkPartition(Set(a), Set(b)), id=id)
    g.add(unique)
    initialTrace += unique
  }

  def reportUnPartition(a: String, b: String, id: Int) {
    val unique = Unique(NetworkUnpartition(Set(a), Set(b)), id=id)
    g.add(unique)
    initialTrace += unique
  }

  def getGraph(): Graph[Unique, DiEdge] = g

  def getInitialTrace(): Queue[Unique] = initialTrace
}
