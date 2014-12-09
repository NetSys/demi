package akka.dispatch.verification


import akka.actor.ActorCell,
       akka.actor.ActorSystem,
       akka.actor.ActorRef,
       akka.actor.Actor,
       akka.actor.PoisonPill,
       akka.actor.Props

import akka.dispatch.Envelope,
       akka.dispatch.MessageQueue,
       akka.dispatch.MessageDispatcher
       
import scala.collection.concurrent.TrieMap,
       scala.collection.mutable.Queue

import scalax.collection.mutable.Graph,
       scalax.collection.GraphPredef._, 
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge,
       scalax.collection.edge.Implicits
       
import java.io.{ PrintWriter, File }

import scalax.collection.edge.LDiEdge,
       scalax.collection.edge.Implicits._,
       scalax.collection.io.dot._

object Util {
    
  
  def queueStr(queue: Queue[(Event, ActorCell, Envelope)]) : String = {
    var str = "Queue content: "
    
    for((item, _ , _) <- queue) item match {
      case m : MsgEvent => str += m.id + " "
    }
    
    return str
  }
    
  
  
  def traceStr(events : Queue[Event]) : String = {
    var str = ""
    for (item <- events) {
      item match {
        case m : MsgEvent => str += m.id + " " 
        case _ =>
      }
    }
    
    return str
  }
  
  
    
  def get_dot(g: Graph[Event, DiEdge]) {
    
    val root = DotRootGraph(
        directed = true,
        id = Some("DPOR"))

    def nodeStr(event: Event) : String = {
      event.value match {
        case msg : MsgEvent => msg.receiver + " (" + msg.id.toString() + ")" 
        case spawn : SpawnEvent => spawn.name + " (" + spawn.id.toString() + ")" 
      }
    }
    
    def nodeTransformer(
        innerNode: scalax.collection.Graph[Event, DiEdge]#NodeT):
        Option[(DotGraph, DotNodeStmt)] = {
      val descr = innerNode.value match {
        case msg : MsgEvent => DotNodeStmt( nodeStr(msg), Seq.empty[DotAttr])
        case spawn : SpawnEvent => DotNodeStmt( nodeStr(spawn), Seq(DotAttr("color", "red")))
      }

      Some(root, descr)
    }
    
    def edgeTransformer(
        innerEdge: scalax.collection.Graph[Event, DiEdge]#EdgeT): 
        Option[(DotGraph, DotEdgeStmt)] = {
      
      val edge = innerEdge.edge

      val src = nodeStr( edge.from.value )
      val dst = nodeStr( edge.to.value )

      return Some(root, DotEdgeStmt(src, dst, Nil))
    }
    
    
    val str = g.toDot(root, edgeTransformer, cNodeTransformer = Some(nodeTransformer))
    
    val pw = new PrintWriter(new File("dot.dot" ))
    pw.write(str)
    pw.close
  }

  
  
  
  def printQueue(queue: Queue[Event]) =
    for (e <- queue) e match {
      case m :MsgEvent => println("\t " + m.id + " " + m.sender + " -> " + m.receiver + " " + m.msg)
      case s: SpawnEvent => println("\t " + s.id + " " + s.name)
    }
  
  
  
  def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
    case null => Array()
    case u: java.net.URLClassLoader => u.getURLs() ++ urlses(cl.getParent)
    case _ => urlses(cl.getParent)
  }
  
    
}