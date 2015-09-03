#!/bin/bash

for branch in raft-45 raft-46 raft-56 raft-58 raft-58-initialization raft-42 raft-66; do
  git checkout $branch
  git pull
  git subtree pull --prefix=interposition interposition master
  git push
done
