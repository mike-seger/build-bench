#!/bin/bash

set -e

if [ $# != 2 ] ; then
	echo "Usage: $0 <server> <reportname>"
	exit 1
fi

server=$1
reportname=$2
mkdir $reportname
rmdir $reportname
ssh $server rm -fR git/build-bench/reports
set +e
ssh $server git/build-bench/run-bench.sh 
echo "benchmark done"
set -e
ssh $server ls -al git/build-bench/reports/stats.txt
scp -r $server:git/build-bench/reports $reportname
echo "copied reports, shutting down vm"
ssh $server sudo shutdown -h now
echo "all finished"
