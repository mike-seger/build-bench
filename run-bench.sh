#!/bin/bash

set -e

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

export TERM=xterm-256color
HH=$(cd; pwd)
source $HH/sdkman/bin/sdkman-init.sh

cd ~/git/build-bench
./multirun.sh >/tmp/1 & 
PID=$!
./watch-stats.sh $PID
