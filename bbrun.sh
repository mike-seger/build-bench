#!/bin/bash

cleanup() {
    # kill all processes whose parent is this process
    pkill -P $$
}

for sig in INT QUIT HUP TERM; do
  trap "
    cleanup
    trap - $sig EXIT
    kill -s $sig "'"$$"' "$sig"
done
trap cleanup EXIT

cd $(dirname "$0")
./multirun.sh >/tmp/1 &
pid=$!
while [ 1==1 ] ; do
	ps aux | grep multirun.sh | egrep -v grep >/dev/null || break
	./watch-stats.sh
	sleep 10 
done
