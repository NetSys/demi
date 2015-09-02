#!/bin/bash

for branch in raft-45 raft-46 raft-56 raft-58 raft-58-initialization raft-42 raft-66; do
  git checkout $branch
  git pull
  sbt assembly && java -d64 -Xmx15g -cp target/scala-2.11/randomSearch-assembly-0.1.jar akka.dispatch.verification.Main > console.out
done
