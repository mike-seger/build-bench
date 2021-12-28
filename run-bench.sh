#!/bin/bash

set -e

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

cd ~/git/build-bench
./multirun.sh >/tmp/1 & 
PID=$!
./watch-stats.sh $PID
