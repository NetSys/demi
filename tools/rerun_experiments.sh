#!/bin/bash

START_WITH=$1

(cd experiments; git pull)

for branch in raft-45 raft-46 raft-56 raft-58 raft-58-initialization raft-42 raft-66; do
  if [ "$START_WITH" != "" -a "$START_WITH" != $branch ]; then
    continue
  fi
  if [ "$START_WITH" != "" -a "$START_WITH" == $branch ]; then
    START_WITH=""
  fi

  echo "==================== Running $branch =================="
  git checkout $branch
  git pull
  sbt assembly && java -d64 -Xmx15g -cp target/scala-2.11/randomSearch-assembly-0.1.jar akka.dispatch.verification.Main > console.out
done
