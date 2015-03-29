#!/usr/bin/env bash

NL=`echo -ne '\015'`
screen -d -m -S raft_session
screen -S raft_session -p 0 -X stuff "python -m eventlet_raft.node t1.conf $NL"
for n in {1..4}; do
  # create now window using `screen` command
  screen -S raft_session -X screen $n
  screen -S raft_session -p $n -X stuff "python -m eventlet_raft.node  t`expr $n + 1`.conf  $NL"
done

screen -r raft_session
