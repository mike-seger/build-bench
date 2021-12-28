#!/bin/bash

set -e

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

expert TERM=xterm-256color

cd ~/git/build-bench
./multirun.sh >/tmp/1 & 
PID=$!
./watch-stats.sh $PID
