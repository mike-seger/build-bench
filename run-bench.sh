#!/bin/bash

set -e

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

cd $(dirname $0)

export TERM=xterm-256color
if [[ ! -x "$(which javac 2>/dev/null)" || ! -x "$(which mvn 2>/dev/null)" ]]; then
  if [ -d "$(cd; pwd)/.sdkman" ] ; then
    source $(cd; pwd)/.sdkman/bin/sdkman-init.sh
  else
    echo "Please install a java jdk 11 and maven 3.8.3"
    exit 1
  fi
fi
if [ -d /x/ ] ; then # Hiren's PE Boot CD
  export HOME=/x
fi

./multirun.sh >/dev/null & 

PID=$!
./watch-stats.sh $PID
