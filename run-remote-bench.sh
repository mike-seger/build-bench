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
ssh $server git/build-bench/run-bench.sh
scp -r $server:git/build-bench/reports $reportname
ssh $server sudo shutdown -h now