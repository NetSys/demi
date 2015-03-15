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
       scala.collection.mutable.Queue,
       scala.collection.mutable.HashMap,
       scala.collection.mutable.Set,
       scala.collection.mutable.ArrayBuffer

import scalax.collection.mutable.Graph,
       scalax.collection.GraphPredef._, 
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge,
       scalax.collection.edge.Implicits
       
import java.io.{ PrintWriter, File }

import scalax.collection.edge.LDiEdge,
       scalax.collection.edge.Implicits._,
       scalax.collection.io.dot._

import akka.cluster.VectorClock
import scala.util.parsing.json.JSONObject
import java.util.Random

// Provides O(1) lookup, but allows multiple distinct elements
class MultiSet[E] extends Set[E] {
  var m = new HashMap[E, List[E]]

  def contains(e: E) : Boolean = {
    return m.contains(e)
  }

  def +=(e: E) : this.type  = {
    if (m.contains(e)) {
      m(e) = e :: m(e)
    } else {
      m(e) = List(e)
    }
    return this
  }

  def -=(e: E) : this.type = {
    if (!m.contains(e)) {
      throw new IllegalArgumentException("No such element " + e)
    }
    m(e) = m(e).tail
    if (m(e).isEmpty) {
      m -= e
    }
    return this
  }

  def iterator: Iterator[E] = {
    return m.values.flatten.iterator
  }
}

// Provides O(1) insert and removeRandomElement
class RandomizedHashSet[E] {
  // We store a counter along with each element E to ensure uniqueness
  var arr = new ArrayBuffer[(E,Int)]
  // Value is index into array
  var hash = new HashMap[(E,Int),Int]
  val rand = new Random(System.currentTimeMillis());
  // This multiset is only used for .contains().. can't use hash's keys since
  // we ensure that they're unique.
  var multiset = new MultiSet[E]

  def insert(value: E) = {
    var uniqueness_counter = 0
    while (hash.contains((value, uniqueness_counter))) {
      uniqueness_counter += 1
    }
    val tuple : (E,Int) = (value,uniqueness_counter)
    val i = arr.length
    hash(tuple) = i
    arr += tuple
    multiset += value
  }

  def remove(value: (E,Int)) = {
    // We are going to replace the cell that contains value in A with the last
    // element in A. let d be the last element in the array A at index m. let
    // i be H[value], the index in the array of the value to be removed. Set
    // A[i]=d, H[d]=i, decrease the size of the array by one, and remove value
    // from H.
    if (!hash.contains(value)) {
      throw new IllegalArgumentException("Value " + value + " does not exist")
    }
    val i = hash(value)
    val m = arr.length - 1
    val d = arr(m)
    arr(i) = d
    hash(d) = i
    arr = arr.dropRight(1)
    hash -= value
    multiset -= value._1
  }

  def contains(value: E) : Boolean = {
    return multiset.contains(value)
  }

  // N.B. if there are duplicated elements, this isn't perfectly random; it
  // will be biased towards duplicates.
  def removeRandomElement () : E = {
    val random_idx = rand.nextInt(arr.length)
    val v = arr(random_idx)
    remove(v)
    return v._1
  }

  // N.B. if there are duplicated elements, this isn't perfectly random; it
  // will be biased towards duplicates.
  def getRandomElement() : E = {
    val random_idx = rand.nextInt(arr.length)
    val v = arr(random_idx)
    return v._1
  }

  def isEmpty () : Boolean = {
    return arr.isEmpty
  }
}

// Used by applications to log messages to the console. Transparently attaches vector
// clocks to log messages.
class VCLogger () {
  var actor2vc = new HashMap[String, VectorClock]

  // TODO(cs): is there a way to specify default values for Maps in scala?
  def ensureKeyExists(key: String) : VectorClock = {
    if (!actor2vc.contains(key)) {
      actor2vc(key) = new VectorClock()
    }
    return actor2vc(key)
  }

  def log(src: String, msg: String) {
    var vc = ensureKeyExists(src)
    // Increment the clock.
    vc = vc :+ src
    // Then print it, along with the message.
    println(JSONObject(vc.versions).toString() + " " + src + ": " + msg)
    actor2vc(src) = vc
  }

  def mergeVectorClocks(src: String, dst: String) {
    val srcVC = ensureKeyExists(src)
    var dstVC = ensureKeyExists(dst)
    dstVC = dstVC.merge(srcVC)
    actor2vc(dst) = dstVC
  }

  def reset() {
    actor2vc = new HashMap[String, VectorClock]
  }
}

object Util {

  
  // Global logger instance.
  val logger = new VCLogger()
    
  def dequeueOne[T1, T2](outer : HashMap[T1, Queue[T2]]) : Option[T2] =
    
    outer.headOption match {
        case Some((receiver, queue)) =>

          if (queue.isEmpty == true) {
            
            outer.remove(receiver) match {
              case Some(key) => dequeueOne(outer)
              case None => throw new Exception("internal error")
            }

          } else { 
            return Some(queue.dequeue())
          }
          
       case None => None
  }

  def getElement[T1](
      container: Option[Queue[T1]],
      condition: T1 => Boolean) : Option[T1] =
    container match {
      case Some(queue) => queue.dequeueFirst(condition)
      case None =>  None
    }

  def queueStr(queue: Queue[(Unique, ActorCell, Envelope)]) : String = {
    var str = "Queue content: "
    
    for((item, _ , _) <- queue) item match {
      case Unique(m : MsgEvent, id) => str += id + " "
    }
    
    return str
  }
    
  
  
  def traceStr(events : Seq[Unique]) : String = {
    var str = ""
    for (item <- events) {
      item match {
        case Unique(_, id) => str += id + " " 
        case _ =>
      }
    }
    
    return str
  }

  
  def traceList(trace: Seq[Unique]) : String = {
    var str = ""
    for (Unique(ev, id) <- trace) {
      str += ev + " " + id + "\n"
    }
    return str
  }
    
  def getDot(g: Graph[Unique, DiEdge]) : String = {
    
    val root = DotRootGraph(
        directed = true,
        id = Some("DPOR"))

    def nodeStr(event: Unique) : String = {
      event.value match {
        case Unique(_, id) => id.toString()
      }
    }
        
    def nodeTransformer(
        innerNode: scalax.collection.Graph[Unique, DiEdge]#NodeT):
        Option[(DotGraph, DotNodeStmt)] = {
      val descr = innerNode.value match {
        case u @ Unique(msg : MsgEvent, id) => DotNodeStmt( nodeStr(u), Seq.empty[DotAttr])
        case u @ Unique(spawn : SpawnEvent, id) => DotNodeStmt( nodeStr(u), Seq(DotAttr("color", "red")))
        case u @ Unique(_, id) => DotNodeStmt( nodeStr(u), Seq.empty[DotAttr])
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
    
    
    return g.toDot(root, edgeTransformer, cNodeTransformer = Some(nodeTransformer))
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
