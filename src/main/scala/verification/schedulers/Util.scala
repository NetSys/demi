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
    
  
  def queueStr(queue: Queue[(Unique, ActorCell, Envelope)]) : String = {
    var str = "Queue content: "
    
    for((item, _ , _) <- queue) item match {
      case Unique(m : MsgEvent, id) => str += id + " "
    }
    
    return str
  }
    
  
  
  def traceStr(events : Queue[Unique]) : String = {
    var str = ""
    for (item <- events) {
      item match {
        case Unique(m : MsgEvent, id) => str += id + " " 
        case _ =>
      }
    }
    
    return str
  }
  
  
    
  def get_dot(g: Graph[Unique, DiEdge]) {
    
    val root = DotRootGraph(
        directed = true,
        id = Some("DPOR"))

    def nodeStr(event: Unique) : String = {
      event.value match {
        case Unique(msg : MsgEvent, id) => msg.receiver + " (" + id + ")" 
        case Unique(spawn : SpawnEvent, id) => spawn.name + " (" + id + ")" 
      }
    }
    
    def nodeTransformer(
        innerNode: scalax.collection.Graph[Unique, DiEdge]#NodeT):
        Option[(DotGraph, DotNodeStmt)] = {
      val descr = innerNode.value match {
        case u @ Unique(msg : MsgEvent, id) => DotNodeStmt( nodeStr(u), Seq.empty[DotAttr])
        case u @ Unique(spawn : SpawnEvent, id) => DotNodeStmt( nodeStr(u), Seq(DotAttr("color", "red")))
      }

      Some(root, descr)
    }
    
    def edgeTransformer(
        innerEdge: scalax.collection.Graph[Unique, DiEdge]#EdgeT): 
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

  
  
  
  def printQueue(queue: Queue[Unique]) =
    for (e <- queue) e match {
      case Unique(m :MsgEvent, id) => println("\t " + id + " " + m.sender + " -> " + m.receiver + " " + m.msg)
      case Unique(s: SpawnEvent, id) => println("\t " + id + " " + s.name)
    }
  
  
  
  def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
    case null => Array()
    case u: java.net.URLClassLoader => u.getURLs() ++ urlses(cl.getParent)
    case _ => urlses(cl.getParent)
  }
  
    
}