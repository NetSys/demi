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
       scala.collection.mutable.HashSet,
       scala.collection.mutable.Set,
       scala.collection.mutable.ArrayBuffer,
       scala.annotation.tailrec

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

class ProvenanceTracker(trace: Queue[Unique], depGraph: Graph[Unique, DiEdge]) {
  val happensBefore = new HashSet[(Unique, Unique)]

  // We know that our traces are linearizable, so we can detect concurrent
  // events without vector clocks.
  //
  // We do this in two steps:
  //  - compute first order happens-before pairs
  //    (Defined as one of two conditions: i. a and b are on the same
  //      machine, or ii. a is a send event and b is the corresponding receive event.)
  //  - Then compute the transitive closure of the initial set of happens-before
  //    pairs
  //
  // N.B. we ignore quiescence and network paritions/unparitions for now.
  {
    // events occuring on the same machine
    var receiver2priorReceives = new HashMap[String, Queue[Unique]]

    println("computing first order happens-before..")
    trace foreach {
      case u @ Unique(MsgEvent(snd, rcv, msg), id) =>
        // deal with prior events on the same machine.
        val priorReceives = receiver2priorReceives.getOrElse(rcv, new Queue[Unique]) += u
        receiver2priorReceives(rcv) = priorReceives
        priorReceives.foreach(p => happensBefore += ((p,u)))
        // deal with message sends that happen as a result of this receive
        // event. (well, we don't explicitly track message sends. But we can
        // look at the the depGraph to know which messages got sent
        // immediately after this receive occurred.)
        depGraph.get(u).inNeighbors.foreach(s => happensBefore += ((u,s)))
      case _ => None
    }

    // We sometimes run into OOMs while computing the transitive closure, so
    // just be extra paranoid about GC.
    receiver2priorReceives = null

    // Now compute the transitive closure.
    // N.B. I initially tried a simple algorithm for computing transitive
    // closure, but it turned out to be hideously slow. So we do something
    // more complicated, described here:
    //   http://cs.stackexchange.com/questions/7231/efficient-algorithm-for-retrieving-the-transitive-closure-of-a-directed-acyclic
    println("computing transitive closure...")

    // First, topologically sort the relation.
    val sorted = Util.topologicalSort[Unique](happensBefore.filter{ case (u1,u2) => u1 != u2 })

    // Now we just go backwards through this list, starting at the last vertex vn.
    // vn's transitive closure is just itself. Also add vn to the transitive closure
    // of every vertex with an edge to vn.
    //
    // For each other vertex vi, going from the end backwards, first add vi to its
    // own transitive closure, then add everything in the transitive closure of vi to
    // the transitive closure of all the vertices with an edge to vi.

    val unique2successors = new HashMap[Unique, Set[Unique]]
    // To make this more efficient, we also track all nodes that have an edge to
    // a successor node
    val node2parents = new HashMap[Unique, Set[Unique]]
    for ((u1, u2) <- happensBefore) {
      node2parents(u2) = node2parents.getOrElse(u2, Set()) + u1
    }

    for (u <- sorted.toSeq.reverse) {
      // First add vi to its own transitive closure.
      unique2successors(u) = unique2successors.getOrElse(u, Set()) + u
      // Then add everything in the transitive closure of vi to
      // the transitive closure of all the vertices with an edge to vi.
      for (parent <- node2parents.getOrElse(u, Set())) {
        unique2successors(parent) = unique2successors.getOrElse(
          parent, Set()) ++ unique2successors(u)
      }
      // Now get rid of unique2successors(u), since we no longer need it.
      for (u2 <- unique2successors(u)) {
        happensBefore += ((u, u2))
      }
      unique2successors -= u
    }
  }

  def concurrent(a: Unique, b: Unique) : Boolean = {
    return !((happensBefore contains (a,b)) || (happensBefore contains (b,a)))
  }

  def pruneConcurrentEvents(violation: ViolationFingerprint) : Queue[Unique] = {
    // First, find the last event that occured in trace for each node affected
    // by the violation
    def findLastEventForNode(node: String) : Option[Unique] = {
      return trace.reverse.find {
        case Unique(MsgEvent(_, rcv, _), _) =>
          rcv == node
        case _ => false
      }
    }

    val lastEvents = violation.affectedNodes.flatMap { findLastEventForNode(_) }

    // Now remove all events that are concurrent or happenAfter all last events
    def concurrentOrAfterAllLastEvents(u: Unique, lastEvents: Seq[Unique]) : Boolean = {
      return lastEvents.forall(o => concurrent(o,u) || happensBefore(o,u))
    }

    // We return those that are *before* lastEvents
    println("computing concurrent events...")
    return trace.filterNot { concurrentOrAfterAllLastEvents(_, lastEvents) }
  }
}

object Util {

  
  // Global logger instance.
  val logger = new VCLogger()

  def map_from_iterable[A,B](in: Iterable[(A,B)]) : collection.mutable.Map[A,B] = {
    val dest = collection.mutable.Map[A,B]()
    for (e @ (k,v) <- in) {
      dest += e
    }

    return dest
  }

  // Taken from: https://gist.github.com/ThiporKong/4399695
  def topologicalSort[A](edges: Traversable[(A, A)]): Iterable[A] = {
    @tailrec
    def tsort(toPreds: Map[A, Set[A]], done: Iterable[A]): Iterable[A] = {
      val (noPreds, hasPreds) = toPreds.partition { _._2.isEmpty }
      if (noPreds.isEmpty) {
        if (hasPreds.isEmpty) done else sys.error(hasPreds.toString)
      } else {
        val found = noPreds.map { _._1 }
        tsort(hasPreds.mapValues { _ -- found }, done ++ found)
      }
    }

    val toPred = edges.foldLeft(Map[A, Set[A]]()) { (acc, e) =>
      acc + (e._1 -> acc.getOrElse(e._1, Set())) + (e._2 -> (acc.getOrElse(e._2, Set()) + e._1))
    }
    tsort(toPred, Seq())
  }

    
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
