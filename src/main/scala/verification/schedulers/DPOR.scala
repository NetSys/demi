package akka.dispatch.verification

import akka.actor.ActorCell,
       akka.actor.ActorSystem,
       akka.actor.ActorRef,
       akka.actor.LocalActorRef,
       akka.actor.ActorRefWithCell,
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
       scala.collection.mutable.ArrayBuffer,
       scala.collection.mutable.ArraySeq,
       scala.collection.mutable.Stack

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge
       
import com.typesafe.scalalogging.LazyLogging,
       org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

       
class ExploredTacker {
  
  var exploredStack = new HashMap[Int, HashSet[(Unique, Unique)] ]

  
  def setExplored(index: Int, pair: (Unique, Unique)) =
  exploredStack.get(index) match {
    case Some(set) => set += pair
    case None =>
      val newElem = new HashSet[(Unique, Unique)] + pair
      exploredStack(index) = newElem
  }
  
  
  def isExplored(pair: (Unique, Unique)): Boolean = {

    for ((index, set) <- exploredStack) set.contains(pair) match {
      case true => return true
      case false =>
    }

    return false
  }

  
  def trimExplored(index: Int) = {
    exploredStack = exploredStack.filter { other => other._1 <= index }
  }

  
  def printExplored() = {
    for ((index, set) <- exploredStack) {
      println(index + ": " + set.size)
    }
  }

}
       

// DPOR scheduler.
class DPOR extends Scheduler with LazyLogging {
  
  final val SCHEDULER = "__SCHEDULER__"
  
  var instrumenter = Instrumenter
  var externalEventList : Seq[ExternalEvent] = Vector()
  var started = false
  
  var currentTime = 0
  var interleavingCounter = 0
  
  val producedEvents = new Queue[Event]
  val consumedEvents = new Queue[Event]
  
  val pendingEvents = new HashMap[String, Queue[(Unique, ActorCell, Envelope)]]  
  val actorNames = new HashSet[String]
 
  val depGraph = Graph[Unique, DiEdge]()
  
  val backTrack = new HashMap[Int, HashMap[(Unique, Unique), List[Unique]] ]
  var invariant : Queue[Unique] = Queue()
  var exploredTacker = new ExploredTacker
  
  val currentTrace = new Queue[Unique]
  val nextTrace = new Queue[Unique]
  var parentEvent = getRootEvent
  
  val reachabilityMap = new HashMap[String, Set[String]]
  
  
  def getRootEvent : Unique = {
    var root = Unique(MsgEvent("null", "null", null), 0)
    depGraph.add(root)
    return root
  }
  
  
  def decomposePartitionEvent(event: NetworkPartition) : Queue[(String, NodesUnreachable)] = {
    val queue = new Queue[(String, NodesUnreachable)]
    queue ++= event.first.map { x => (x, NodesUnreachable(event.second)) }
    queue ++= event.second.map { x => (x, NodesUnreachable(event.first)) }
    return queue
  }
  
  
  def isSystemCommunication(sender: ActorRef, receiver: ActorRef) : Boolean =
    throw new Exception("not implemented")

  
  override def isSystemCommunication(sender: ActorRef, receiver: ActorRef, msg: Any): Boolean = 
  (receiver, sender) match {
    case (null, _) => return true
    case (_, null) => isSystemMessage("deadletters", receiver.path.name, msg)
    case _ => isSystemMessage(sender.path.name, receiver.path.name, msg)
  }

  
  // Is this message a system message
  def isValidActor(sender: String, receiver: String): Boolean = 
  ((actorNames contains sender) || (actorNames contains receiver)) match {
    case true => return true
    case _ => return false
  }


  
  def ignoreMsg[T](t: T): Boolean = t match {
    case msg : NodesUnreachable => return true
    case _ => return false
  }
  
  
  def isSystemMessage(sender: String, receiver: String) : Boolean =
    throw new Exception("not implemented")
  
  // Is this message a system message
  override def isSystemMessage(sender: String, receiver: String, msg: Any): Boolean = {
    return ignoreMsg(msg) || !isValidActor(sender, receiver)
  }
  
  
  // Notification that the system has been reset
  def start_trace() : Unit = {
    
    started = false
    actorNames.clear
    
    runExternal()
  }
  
  
  // When executing a trace, find the next trace event.
  def mutableTraceIterator( trace: Queue[Unique]) : Option[Unique] =
  trace.isEmpty match {
    case true => return None
    case _ => return Some(trace.dequeue)
  }
  
  

  // Get next message event from the trace.
  def get_next_trace_message() : Option[Unique] =
  mutableTraceIterator(nextTrace) match {
    case some @ Some(Unique(m : MsgEvent, 0)) => get_next_trace_message()
    case some @ Some(Unique(m : MsgEvent, id)) => some
    case some @ Some(Unique(s: SpawnEvent, id)) => get_next_trace_message()
    case _ => None
  }

  
  
  
  // Figure out what is the next message to schedule.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    
    // Filter messages belonging to a particular actor.
    def is_the_same(u1: Unique, other: (Unique, ActorCell, Envelope)) : 
    Boolean = (u1, other) match {
      
      case (Unique(MsgEvent(snd1, rcv1, msg1), id1), 
            (Unique(MsgEvent(snd2, rcv2, msg2), id2) , cell, env) ) =>
              
        if (id1 == 0) rcv1 == cell.self.path.name
        else rcv1 == cell.self.path.name && id1 == id2
        
      case (Unique(_, id1), (Unique(_, id2) , _, _) ) => id1 == id2  
      case _ => false
    }


    // Get from the current set of pending events.
    def get_pending_event(): Option[(Unique, ActorCell, Envelope)] = {
      // Do we have some pending events
      pendingEvents.headOption match {
        case Some((receiver, queue)) =>

          if (queue.isEmpty == true) {
            
            pendingEvents.remove(receiver) match {
              case Some(key) => get_pending_event()
              case None => throw new Exception("internal error")
            }

          } else {
            
            queue.dequeue() match {
              
              case next @ (Unique(MsgEvent(snd, rcv, msg), id), cell, env) =>
                logger.trace( Console.GREEN + "Now playing pending: " 
                    + "(" + snd + " -> " + rcv + ") " +  + id + Console.RESET )
                Some(next)
                
              case par @ (Unique(NetworkPartition(part1, part2), id), _, _) =>
                logger.trace( Console.GREEN + "Now playing the high level partition event." +
                    Console.RESET)
                Some(par)
            }
          }
          
        case None => None
      }
    }
    
    
    def getMatchingMessage() : Option[(Unique, ActorCell, Envelope)] = 
      
      get_next_trace_message() match {
        // The trace says there is a message event to run.
        case Some(u @ Unique(MsgEvent(snd, rcv, msg), id)) =>
          
          // Look at the pending events to see if such message event exists. 
          pendingEvents.get(rcv) match {
            case Some(queue) => queue.dequeueFirst(is_the_same(u, _))
            case None =>  None
          }
          
        case Some(u @ Unique(NetworkPartition(_, _), id)) =>
          
          // Look at the pending events to see if such message event exists. 
          pendingEvents.get(SCHEDULER) match {
            case Some(queue) => queue.dequeueFirst(is_the_same(u, _))
            case None =>  None
          }
          
        // The trace says there is nothing to run so we have either exhausted our
        // trace or are running for the first time. Use any enabled transitions.
        case _ => None
      }
    
    
    val result = getMatchingMessage() match {
      
      // There is a pending event that matches a message in our trace.
      // We call this a convergent state.
      case m @ Some((u @ Unique(MsgEvent(snd, rcv, msg), id), cell, env)) =>
        logger.trace( Console.GREEN + "Replaying the exact message: " +
            "(" + snd + " -> " + rcv + ") " +  + id + Console.RESET )
        Some((u, cell, env))
        
      case Some((u @ Unique(NetworkPartition(part1, part2), id), _, _)) =>
        logger.trace( Console.GREEN + "Partitioning " + part1 + 
            " and " + part2 + Console.RESET )
        Some((u, null, null))
        
      // We call this a divergent state.
      case None => get_pending_event()
      
      // Something went wrong.
      case _ => throw new Exception("not a message")
    }
    
    

    result match {
      
      case Some((nextEvent @ Unique(MsgEvent(snd, rcv, msg), nID), cell, env)) =>

        invariant.headOption match {
          case Some(Unique(m : MsgEvent, invID)) if (nID == invID) =>
            logger.trace( Console.RED + "Managed to replay the intended message: " +
            "(" + snd + " -> " + rcv + ") " +  + nID + Console.RESET )
            invariant.dequeue()
          case _ =>
        }
        
        currentTrace += nextEvent
        (depGraph get nextEvent)
        parentEvent = nextEvent
        return Some((cell, env))
        
        
      case Some((nextEvent @ Unique(par : NetworkPartition, nID), _, _)) =>
        
        // A NetworkPartition event is translated into multiple
        // NodesUnreachable messages which are atomically and
        // and invisibly consumed by all relevant parties.
        // Important: no messages are allowed to be dispatched
        // as the result of NodesUnreachable being received.
        val internalMsgs = decomposePartitionEvent(par)
        internalMsgs.map{ case (rcv, msg) => instrumenter().actorMappings(rcv) ! msg}
        currentTrace += nextEvent
        return schedule_new_message()
        
      case _ => return None
    }

    
  }
  
  
  // XXX: Not used
  def next_event() : Event = {
    throw new Exception("not implemented next_event()")
  }

  
  // Record that an event was consumed
  def event_consumed(event: Event) = 
    consumedEvents.enqueue(event)
  
  
  def event_consumed(cell: ActorCell, envelope: Envelope) = {
    var event = new MsgEvent(
        envelope.sender.path.name, cell.self.path.name, 
        envelope.message)
    
    consumedEvents.enqueue( event )
  }
  
  
  // Record that an event was produced 
  def event_produced(event: Event) = {
        
    event match {
      case event : SpawnEvent => actorNames += event.name
      case msg : MsgEvent => 
    }
    
    producedEvents.enqueue( event )
  }
  
  
  def runExternal() = {
    currentTrace += getRootEvent
    
    for(event <- externalEventList) event match {
      case Start(props, name) => 
        instrumenter().actorSystem().actorOf(props, name)
  
      case Send(rcv, msg) =>
        val ref = instrumenter().actorMappings(rcv)
        instrumenter().actorMappings(rcv) ! msg
        
      case NetworkPartition(part1, part2) =>
        val msgs = pendingEvents.getOrElse(SCHEDULER, new Queue[(Unique, ActorCell, Envelope)])
        pendingEvents(SCHEDULER) = msgs += ((
            Unique(NetworkPartition(part1, part2)), null, null))
        
      case _ => throw new Exception("unsuported external event")
    }
    
    instrumenter().tellEnqueue.await()
    
    schedule_new_message() match {
      case Some((cell, env)) =>
        instrumenter().dispatch_new_message(cell, env)
      case None => 
        throw new Exception("internal error")
    }
  }
        

  def run(externalEvents: Seq[ExternalEvent]) = {
    externalEventList = externalEvents
    instrumenter().reinitialize_system(null, null)
  }
  
  
  def getMessage(cell: ActorCell, envelope: Envelope) : Unique = {
    
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = new MsgEvent(snd, rcv, envelope.message)
    
    val parent = parentEvent match {
      case u @ Unique(m: MsgEvent, id) => u
      case _ => throw new Exception("parent event not a message")
    }

    val inNeighs = depGraph.get(parent).inNeighbors
    inNeighs.find { x => x.value.event == msg } match {
      
      case Some(x) => return x.value
      case None =>
        val newMsg = Unique( MsgEvent(msg.sender, msg.receiver, msg.msg) )
        logger.trace(
            Console.YELLOW + "Not seen: " + newMsg.id + 
            " (" + msg.sender + " -> " + msg.receiver + ") " + Console.RESET)
        return newMsg
      case _ => throw new Exception("wrong type")
    }
      
  }
  
  
  
  def event_produced(cell: ActorCell, envelope: Envelope) = {

    val unique @ Unique(msg : MsgEvent , id) = getMessage(cell, envelope)
    val msgs = pendingEvents.getOrElse(msg.receiver, new Queue[(Unique, ActorCell, Envelope)])
    pendingEvents(msg.receiver) = msgs += ((unique, cell, envelope))
    
    logger.trace(Console.BLUE + "New event: " +
        "(" + msg.sender + " -> " + msg.receiver + ") " +
        id + Console.RESET)
    
    depGraph.add(unique)
    producedEvents.enqueue( msg )

    depGraph.addEdge(unique, parentEvent)(DiEdge)

  }
  
  
  // Called before we start processing a newly received event
  def before_receive(cell: ActorCell) {
  }
  
  
  // Called after receive is done being processed 
  def after_receive(cell: ActorCell) {
  }
  

  
  def printPath(path : List[depGraph.NodeT]) : String = {
    var pathStr = ""
    for(node <- path) {
      node.value match {
        case Unique(m : MsgEvent, id) => pathStr += id + " "
        case _ => throw new Exception("internal error not a message")
      }
    }
    return pathStr
  }

  
  
  def notify_quiescence() {
    
    logger.info("\n--------------------- Interleaving #" +
                interleavingCounter + " ---------------------")
    
    logger.debug(Console.BLUE + "Current trace: " +
        Util.traceStr(currentTrace) + Console.RESET)

    
    nextTrace.clear()
    nextTrace ++= dpor(currentTrace)
    
    logger.debug(Console.BLUE + "Next trace:  " + 
        Util.traceStr(nextTrace) + Console.RESET)
        
    producedEvents.clear()
    consumedEvents.clear()
  
    currentTrace.clear
    
    parentEvent = getRootEvent

    pendingEvents.clear()

    instrumenter().await_enqueue()
    instrumenter().restart_system()
  }
  
  
  
  def getEvent(index: Integer, trace: Queue[Unique]) : Unique = {
    trace(index) match {
      case u: Unique => u 
      case _ => throw new Exception("internal error not a message")
    }
  }
  

  def dpor(trace: Queue[Unique]) : Queue[Unique] = {
    
    interleavingCounter += 1
    val root = getEvent(0, currentTrace)
    val rootN = ( depGraph get getRootEvent )
    
    val racingIndices = new HashSet[Integer]
    
    /**
     * Analyze the dependency between two events that are co-enabled
     * * and have the same receiver.
     *
     * * @param earleirI: Index of the earlier event.
     * * @param laterI: Index of the later event.
     * * @param trace: The trace to which the events belong to.
     *
     * * @return none
     */
    def analyize_dep(earlierI: Int, laterI: Int, trace: Queue[Unique]): 
    Option[(Int, List[Unique])] = {

      // Retrieve the actual events.
      val earlier = getEvent(earlierI, trace)
      val later = getEvent(laterI, trace)

      // See if this interleaving has been explored.
      //val explored = alreadyExplored.contains((later, earlier))
      val explored = exploredTacker.isExplored((later, earlier))
      if (explored) return None

      (earlier.event, later.event) match {
        
        case (_: MsgEvent,_: NetworkPartition) =>
          val branchI = earlierI
          val needToReplay = List(later, earlier)
          
          return Some((branchI, needToReplay))
          
        case (_: NetworkPartition, _: MsgEvent) => 
          val branchI = earlierI - 1
          val needToReplay = currentTrace.clone()
            .drop(earlierI + 1)
            .take(laterI - earlierI)
            .toList :+ earlier
                      
          return Some((branchI, needToReplay))
          
        case (_: MsgEvent, _: MsgEvent) =>
          // Get the actual nodes in the dependency graph that
          // correspond to those events
          val earlierN = (depGraph get earlier)
          val laterN = (depGraph get later)

          // Get the dependency path between later event and the
          // root event (root node) in the system.
          val laterPath = laterN.pathTo(rootN) match {
            case Some(path) => path.nodes.toList.reverse
            case None => throw new Exception("no such path")
          }

          // Get the dependency path between earlier event and the
          // root event (root node) in the system.
          val earlierPath = earlierN.pathTo(rootN) match {
            case Some(path) => path.nodes.toList.reverse
            case None => throw new Exception("no such path")
          }

          // Find the common prefix for the above paths.
          val commonPrefix = laterPath.intersect(earlierPath)

          // Figure out where in the provided trace this needs to be
          // replayed. In other words, get the last element of the
          // common prefix and figure out which index in the trace
          // it corresponds to.
          val lastElement = commonPrefix.last
          val branchI = trace.indexWhere { e => (e == lastElement.value) }

          val needToReplay = currentTrace.clone()
            .drop(branchI + 1)
            .dropRight(currentTrace.size - laterI - 1)
            .filter { x => x.id != earlier.id }

          require(branchI < laterI)

          // Since we're dealing with the vertices and not the
          // events, we need to extract the values.
          val needToReplayV = needToReplay.toList

          // Since we're exploring an already executed trace, we can
          // safely mark the interleaving of (earlier, later) as
          // already explored.
          exploredTacker.setExplored(branchI, (earlier, later))

          logger.trace(Console.CYAN + "Replay:  " +
            Util.traceStr(needToReplay) + Console.RESET)
          logger.info(Console.GREEN +
            "Found a race between " + earlier.id + " and " +
            later.id + " with a common index " + branchI +
            Console.RESET)

          return Some((branchI, needToReplayV))

      }

    }
    
    
    /** Figure out if two events are co-enabled.
     *
     * See if there is a path from the later event to the
     * earlier event on the dependency graph. If such
     * path does exist, this means that one event disables
     * the other one.
     * 
     ** @param earlier: First event
     ** @param later: Second event
     * 
     ** @return: Boolean 
     */
    def isCoEnabeled(earlier: Unique, later: Unique) : Boolean = (earlier, later) match {
      
      case (Unique(p : NetworkPartition, _), _) => true
      case (_, Unique(p : NetworkPartition, _)) => true
      //case (_, _) =>
      case (Unique(m1 : MsgEvent, _), Unique(m2 : MsgEvent, _)) =>
        if (m1.receiver != m2.receiver) 
          return false
        val earlierN = (depGraph get earlier)
        val laterN = (depGraph get later)
        
        val coEnabeled = laterN.pathTo(earlierN) match {
          case None => true
          case _ => false
        }
        
        return coEnabeled
    }
    

    /*
     * For every event in the trace (called later),
     * see if there is some earlier event, such that:
     * 
     * 0) They belong to the same receiver.
     * 1) They are co-enabled.
     * 2) Such interleaving hasn't been explored before.
     * 3) There is not a freeze flag associated with their
     *    common backtrack index.
     */ 
    for(laterI <- 0 to trace.size - 1) {
      val later @ Unique(laterEvent, laterID) = getEvent(laterI, trace)

      for(earlierI <- 0 to laterI - 1) {
        val earlier @ Unique(earlierEvent, earlierID) = getEvent(earlierI, trace) 
        
        //val sameReceiver = earlierMsg.receiver == laterMsg.receiver
        if ( isCoEnabeled(earlier, later)) {
          
          analyize_dep(earlierI, laterI, trace) match {
            case Some((branchI, needToReplayV)) => 
              val racingPair = ((later, earlier))
              backTrack.getOrElseUpdate(branchI, new HashMap[(Unique, Unique), List[Unique]])
              backTrack(branchI)(racingPair) = needToReplayV
              
            case None => // Nothing
          }
          
        }
        
      }
    }
    
    // If the backtrack set is empty, this means we're done.
    if (backTrack.isEmpty) {
      logger.info("Tutto finito!")
      System.exit(0);
    }

    // Find the deepest backtrack value, and make sure
    // its index is removed from the freeze set.
    val maxIndex = backTrack.keySet.max
    val (_ @ Unique(_, id1),
         _ @ Unique(_, id2)) = backTrack(maxIndex).head match {
      case ((u1, u2), eventList) => (u1, u2)
      case _ => throw new Exception("invalid interleaving event types")
    }
    
    logger.info(Console.RED + "Exploring a new message interleaving " + 
       id1 + " and " + id2  + " at index " + maxIndex + Console.RESET)
        
    val ((e1, e2), replayThis) = backTrack(maxIndex).head
    
    backTrack(maxIndex).remove((e1, e2))
    
    exploredTacker.setExplored(maxIndex, (e1, e2))
    exploredTacker.trimExplored(maxIndex)
    exploredTacker.printExplored()
    
    // A variable used to figure out if the replay diverged.
    invariant = Queue(e1, e2)
    
    // Remove the backtrack branch, since we're about explore it now.
    if (backTrack(maxIndex).isEmpty)
      backTrack -= maxIndex
    
    // Return all events up to the backtrack index we're interested in
    // and slap on it a new set of events that need to be replayed in
    // order to explore that interleaving.
    return trace.take(maxIndex + 1) ++ replayThis
    
  }
  

}