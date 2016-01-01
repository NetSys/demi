package akka.dispatch.verification

import akka.actor.{Cell, ActorRef}
import scala.tools.jline
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Queue

import java.util.concurrent.atomic.AtomicBoolean

import akka.dispatch.Envelope

sealed trait JLineEvent
case class Line(value: String) extends JLineEvent
case object EmptyLine extends JLineEvent
case object EOF extends JLineEvent

// Defines an argument type, with help message.
// Does not contain values for arguments; see BoundDemiCommand.
case class DemiCommandArg(val name: String, help_msg:String="") {
  def arg_help = help_msg
}

// Defines a command type, with help message.
class DemiCommand(val name: String, val alias:String="", help_msg:String="") {
  var args = new ListBuffer[DemiCommandArg]

  def arg(name: String, help_msg:String=""): this.type = {
    args += DemiCommandArg(name, help_msg)
    return this
  }

  def arg_help =
    for (a <- args if a.arg_help != "") yield s"  <${a.name}>: ${a.arg_help}"

  def get_help: String = {
    var argsStr = args.map(i => s"<${i.name}>").mkString(" ")
    var name_with_args = s"$name $argsStr"
    var aliasStr = if (alias != "") s"[$alias]" else ""
    var args_help = if (!arg_help.isEmpty) arg_help.mkString("\n") else ""
    var formatStr = f"$name_with_args%34s $aliasStr%8s $help_msg%20s"
    if (args_help != "") formatStr = formatStr + f"%n $args_help%20s"
    return formatStr
  }
}

// Defines an instantiated command, with optional values for arguments.
case class BoundDemiCommand(cmdType: DemiCommand, args:Seq[String]=Seq.empty)

class DemiConsole {
  //var call_env = new HashMap[String,() => Unit]
  var help_items = new ListBuffer[DemiCommand]
  var commands = new HashMap[String, DemiCommand]
  val help = BoundDemiCommand(cmd("help", alias="h", help_msg="This help screen"))
  val quit = BoundDemiCommand(cmd("quit", alias="q", help_msg="Exit the program"))

  def printHeader() {
    println(Console.WHITE)
    println("DEMi Interactive console.")
    println("Python expressions and DEMi commands supported.")
    println("Type 'help' for an overview of available commands.")
    println(Console.RESET)
  }

  def show_help(command:String="") {
    if (command != "") {
      if (commands contains command) {
        var name = s"Command $command"
        var equals = "=" * ((90-name.length)/2)
        println(s"$equals $name $equals")
        println(commands(command).get_help)
      } else {
        println(s"Command $command not found")
      }
    } else {
      var name = "Command Help"
      var equals = "=" * ((90-name.length)/2)
      println(s"$equals $name $equals")
      help_items.foreach { case i => println(i.get_help) }
    }
  }

  //def cmd_group(name:String) {
  //  var equals = "-" * ((90-len(name))/2)
  //  help_items += s"\n$equals $name $equals"
  //}

  // def _register(func: () => Unit, name: String, alias:String="") {
  //   call_env(name) = func
  //   if (alias != "") {
  //     for (key <- alias.split('|'))
  //       call_env(key) = func
  //   }
  // }

  def cmd(name: String, alias:String="",
          help_msg:String=""): DemiCommand = {
    //_register(func, name, alias)
    var command = new DemiCommand(name, alias, help_msg)
    commands(name) = command
    if (alias != "") {
      for (key <- alias.split('|'))
        commands(key) = command
    }
    help_items += command
    return command
  }

  def autocomplete_matches(text: String, index: Int): Seq[String] = {
    if (index == 0) {
      return for (s <- commands.keys.toSeq if s.startsWith(text)) yield s
    } else {
      /*
      if parts[0] in commands:
        command = commands[parts[0]]
        argno = len(parts) - 2
        argval = parts[-1]
        if argno < len(command.args):
          return [ s for s in args[argo].values() if s.startswith(argval) ]
      */
      return Seq.empty
    }
  }

  // Handler should return true if we're done reading this input line, or
  // false if this input line was invalid.
  def readline( handler: JLineEvent => Boolean ) {
    // TODO(cs): register auto-completer
    val consoleReader = new jline.console.ConsoleReader()
    var finished = false
    while (!finished) {
      val line = consoleReader.readLine(Console.WHITE + " > " + Console.RESET)
      if (line == null) {
        finished = handler( EOF )
      } else {
        val trimmed = line.trim
        if (trimmed.size == 0) {
          finished = handler( EmptyLine )
        } else if (trimmed.size > 0) {
          finished = handler( Line( trimmed ) )
        }
      }
    }
  }

  def run(): BoundDemiCommand = {
    //local = call_env
    //def completer(text, state) {
    //  try:
    //    matches = autocomplete_matches(text, readline.get_begidx())
    //    if state < len(matches):
    //        return matches[state]
    //    else:
    //        return None
    //  except BaseException as e:
    //    print "Error on autocompleting: %s" % e
    //    raise e
    //}

    //readline.set_completer(completer)
    //digits = '\d+'.r
    //quoted = '".*"|\'.*\''.r

    //def quote_parameter(s:String): String = {
    //  s match {
    //    case quoted =>
    //      return s
    //    case digits =>
    //      return s
    //    case _ =>
    //      val quoted = s.replace("'", "\\'")
    //      return s"'${quoted}'"
    //  }
    //}

    def input(): BoundDemiCommand = {
      var ret : BoundDemiCommand = help
      readline {
        case EOF =>
          ret = quit
          true
        case Line(s) =>
          val parts = s.split(" ")
          val cmd_name = parts(0)
          if (commands contains cmd_name) {
            val args = parts.slice(1,parts.length)
            ret = BoundDemiCommand(commands(cmd_name), args)
            true
          } else {
            println(s"Invalid command: $s")
            false
          }
        case _ =>
          false
      }
      return ret
    }

    val command = input()
    return command
  }
}

/**
 * Provide a command-line interface for interactively choosing the next
 * message to schedule.
 */
class InteractiveScheduler(val schedulerConfig: SchedulerConfig)
  extends AbstractScheduler with ExternalEventInjector[ExternalEvent] {

  val shuttingDown = new AtomicBoolean(false)

  // Current set of enabled events.
  val pendingEvents = new MultiSet[Uniq[(Cell, Envelope)]]
  // Pending failure detector and checkpoint messages.
  var pendingSystemMessages = new Queue[Uniq[(Cell, Envelope)]]

  val console = new DemiConsole
  console.printHeader
  val deliverEventCmd = console.cmd("deliver", "d", "Deliver the pending event with the given id")
                            .arg("id", "id of pending event [integer].")
  val checkInvariantCmd = console.cmd("inv", "i", "Check the given invariant")

  var test_invariant : TestOracle.Invariant = schedulerConfig.invariant_check match {
    case Some(i) => i
    case None => null
  }

  var externals : Seq[ExternalEvent] = null

  def run(_externals:Seq[ExternalEvent]): Tuple2[EventTrace,Option[ViolationFingerprint]] = {
    externals = _externals
    // TODO(cs): track violation.
    Instrumenter().scheduler = this
    // TODO(cs): ? reset_all_state
    return (execute_trace(_externals), None)
  }

  // Record a mapping from actor names to actor refs
  override def event_produced(event: Event) = {
    super.event_produced(event)
    handle_spawn_produced(event)
  }

  // Record that an event was consumed
  override def event_consumed(event: Event) = {
    handle_spawn_consumed(event)
  }

  // Record a message send event
  override def event_consumed(cell: Cell, envelope: Envelope) = {
    handle_event_consumed(cell, envelope)
  }

  // TODO(cs): redundant with RandomScheduler. Factor out.
  override def event_produced(cell: Cell, envelope: Envelope) = {
    val rcv = cell.self.path.name
    var snd = envelope.sender.path.name
    val msg = envelope.message
    val uniq = Uniq[(Cell, Envelope)]((cell, envelope))

    if (MessageTypes.fromFailureDetector(msg) ||
        MessageTypes.fromCheckpointCollector(msg)) {
      pendingSystemMessages += uniq
    } else if (rcv == CheckpointSink.name && schedulerConfig.enableCheckpointing) {
      checkpointer.handleCheckpointResponse(envelope.message, snd)
      if (checkpointer.done && !blockedOnCheckpoint.get) {
        test_invariant(externals, checkpointer.checkpoints) match {
          case None =>
            println(Console.YELLOW + "No Violation" + Console.RESET)
          case Some(fingerprint) =>
            println(Console.RED + s"Violation found: $fingerprint" + Console.RESET)
        }
      }
    } else {
      pendingEvents += uniq
    }
  }

  def deliverEvent(args: Seq[String], aliasToId: HashMap[Char,Int]): Option[(Cell, Envelope)] = {
    var ret: Option[(Cell, Envelope)] = None
    if (args.length != 1) {
      println(s"Should only have one argument. Was: $args")
    } else {
      val alias = args(0).head
      if (!(aliasToId contains alias)) {
        println(s"No such event $alias")
      }
      val id = aliasToId(alias)
      pendingEvents.find(e => e.id == id) match {
        case Some(e) =>
          ret = Some(e.element)
          pendingEvents -= e
        case None =>
          println(s"No such event $alias")
      }
    }
    return ret
  }

  // TODO(cs): factor me into ExternalEventInjector
  def checkInvariant() {
    if (!schedulerConfig.enableCheckpointing) {
      // If no checkpointing, go ahead and check the invariant now
      var checkpoint = new HashMap[String, Option[CheckpointReply]]
      test_invariant(externals, checkpoint) match {
        case None =>
          println(Console.YELLOW + "No Violation" + Console.RESET)
        case Some(fingerprint) =>
          println(Console.RED + s"Violation found: $fingerprint" + Console.RESET)
      }
    } else {
      prepareCheckpoint()
    }
  }

  // Figure out what is the next message to schedule.
  def schedule_new_message(blockedActors: Set[String]) : Option[(Cell, Envelope)] = {
    if (shuttingDown.get()) return None

    send_external_messages()

    if (!pendingSystemMessages.isEmpty) {
      // Find a non-blocked destination
      Util.find_non_blocked_message[Uniq[(Cell, Envelope)]](
        blockedActors,
        pendingSystemMessages,
        () => pendingSystemMessages.dequeue(),
        (e: Uniq[(Cell, Envelope)]) => e.element._1.self.path.name) match {
        case Some(uniq) => return Some(uniq.element)
        case _ =>
      }
    }

    // Assign mappings from unique id <-> char
    val idToAlias = new HashMap[Int,Char]
    val aliasToId = new HashMap[Char,Int]

    {
    var currentAlias = 'a'
    pendingEvents.foreach { case uniq =>
      idToAlias(uniq.id) = currentAlias
      aliasToId(currentAlias) = uniq.id
      currentAlias = (currentAlias + 1).toChar
    }
    }

    def printEventsWithIds() {
      println("Pending events:")
      pendingEvents.foreach { case uniq =>
        val cell = uniq.element._1
        val envelope = uniq.element._2
        val snd = envelope.sender.path.name
        val rcv = cell.self.path.name
        val msg = envelope.message
        val alias = idToAlias(uniq.id)
        println(Console.GREEN + f"[$alias]:    $snd -> $rcv: $msg" + Console.RESET)
      }
    }

    var finished = false
    var ret: Option[(Cell, Envelope)] = None
    while (!finished) {
      printEventsWithIds()
      val command = console.run()
      command match {
        case console.help =>
          console.show_help()
        case console.quit =>
          finished = true
          shuttingDown.set(true)
        case BoundDemiCommand(`deliverEventCmd`, args) =>
          ret = deliverEvent(args, aliasToId)
          if (!ret.isEmpty) {
            finished = true
          }
        case BoundDemiCommand(`checkInvariantCmd`, args) =>
          checkInvariant()
          if (schedulerConfig.enableCheckpointing) {
            send_external_messages()

            // Find a non-blocked destination
            Util.find_non_blocked_message[Uniq[(Cell, Envelope)]](
              blockedActors,
              pendingSystemMessages,
              () => pendingSystemMessages.dequeue(),
              (e: Uniq[(Cell, Envelope)]) => e.element._1.self.path.name) match {
              case Some(uniq) => return Some(uniq.element)
              case _ =>
            }
          }
        // TODO(cs): inject external?
        case _ =>
          println(s"Unknown command: $command")
      }
    }
    return ret
  }

  override def notify_quiescence () {
    if (shuttingDown.get()) {
      traceSem.release()
    }
  }

  def notify_timer_cancel(rcv: String, msg: Any) {
    pendingEvents.retain(uniq =>
      uniq.element._2.message != msg &&
      uniq.element._1.self.path.name != rcv)
  }

  // Shutdown the scheduler, this ensures that the instrumenter is returned to its
  // original pristine form, so one can change schedulers
  override def shutdown () = {
    handle_shutdown
  }

  // Notification that the system has been reset
  override def start_trace() : Unit = {
    handle_start_trace
  }

  override def before_receive(cell: Cell) : Unit = {
    handle_before_receive(cell)
  }

  override def after_receive(cell: Cell) : Unit = {
    handle_after_receive(cell)
  }

  // TODO(cs): enqueue_code_block, enqueue_timer, handleMailboxIdle?

  override def reset_all_state () = {
    super.reset_all_state
    pendingEvents.clear
  }
}
