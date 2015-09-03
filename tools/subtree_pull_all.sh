#!/bin/bash

RUN_BASH=$1

for branch in raft-45 raft-46 raft-56 raft-58 raft-58-initialization raft-42 raft-66; do
  git checkout $branch
  git pull
  git subtree pull --prefix=interposition interposition master
  if [ "$RUN_BASH" != "" ]; then
    bash
  fi
  git push
done
