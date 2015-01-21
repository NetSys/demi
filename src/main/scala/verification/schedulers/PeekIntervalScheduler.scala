/*
    def containedInExpected(msgEvent: MsgEvent) : Boolean = {
      val found = expected.find(e =>
        e match {
          case MsgEvent(sender, receiver, msg) =>
            msgEvent.sender == sender && msgEvent.receiver == receiver && msgEvent.msg == msg
          case _ => false
        }
      )
      return found != None
    }

    val prefix = new ListBuffer[MsgEvent]
    var triedSoFar = 0
    // STSScheduler.maxPeekMessagesToTry

    // TODO(cs): at this point the actor system has already been started, so
    // I'm not sure this is going to work.
    // val peeker = new PeekScheduler
    // Instrumenter().scheduler = peeker
    // val peekedEvents = peeker.peek(subseq)
    // assume(!peekedEvents.isEmpty)
    // assume(EventTypes.externalEventTypes.contains(peekedEvents(0).getClass))
    // peeker.shutdown
    // Instrumenter().scheduler = this
    // return peekedEvents
*/
