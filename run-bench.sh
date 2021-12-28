#!/bin/bash

set -e

cd ~/git/build-bench
./multirun.sh >/tmp/1 & 
PID=$?
./watch-stats.sh $PID
