#!/bin/bash

set -e

cd ~/git/build-bench
./multirun.sh >/tmp/1 & 
./watch-stats.sh
