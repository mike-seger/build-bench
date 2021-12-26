#!/bin/bash

set -e

parallel=$1
cd $(dirname "$0")

if [ -d /mnt/ramdisk ] ; then
	export HOME=/mnt/ramdisk
fi

mkdir -p reports

./run.sh dl $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
#./run.sh $parallel

./stats.sh -f >reports/stats.txt
