# DEMi

This is the home of Distributed Execution Minimizer (DEMi), a fuzzing and test
case reducing tool for distributed systems. Currently we only support applications
built on top of [Akka](http://akka.io/), but adapting another RPC library to
call into DEMi shouldn't be too difficult.

## Useful resources:

- [Blog post](http://colin-scott.github.io/blog/2015/10/07/fuzzing-raft-for-fun-and-profit) covering how DEMi does fuzz testing, and the bugs we found in a Raft implementation using DEMi.
- Our NSDI 2016 [paper](http://eecs.berkeley.edu/~rcs/research/nsdi16.pdf) describing the system in detail.
- Example applications tested with DEMi can be found [here](https://github.com/NetSys/demi-applications).

## Current status of this project

Although DEMi's features are fairly well fleshed out (e.g., we've used it to test [Spark](http://spark.apache.org/)), so far it has only been used by us.
That is to say, there isn't a whole lot of documentation.

If you're interested in using DEMi, we'd be more than happy to write up
documentation, and help you iron out any issues you run into. We'd love to see
DEMi applied more broadly!

## Whirlwind tour of how to use DEMi

For now, a whirwind tour:

 - this [repository](https://github.com/NetSys/demi) contains the tool itself
 - an application pulls in DEMi by having it as a git subtree, located at `interposition/`. See the various branches of this [repository](https://github.com/NetSys/demi-applications) for examples.
 - in the application's Build.scala (or build.sbt), we need to include AspectJ, through the sbt-aspectj plugin. Here is an example: https://github.com/NetSys/demi-applications/blob/raft-45/project/Build.scala#L3. When we run `sbt build` from the application's top-level directory, AspectJ interposition will be automatically weaved in. The AspectJ code itself is [here](https://github.com/NetSys/demi/blob/master/src/main/aspectj/WeaveActor.aj). It mostly weaves interfaces within Akka, and should hopefully work on most versions of higher than 2.0.
 - In the application's Main method, we need to configure DEMi . For now, here is very verbose [example](https://github.com/NetSys/demi-applications/blob/raft-45/src/main/scala/pl/project13/Runner.scala). That example contains a whole bunch of cruft that is needed for minimization (+ other auxiliary stuff like recording the execution, writing it to disk, and replaying it later), but is not needed for simple fuzz testing. For just fuzz testing, you should be able to do it in a few lines of code. I'd be glad to come up with an example if you're interested.

