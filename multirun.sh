#!/bin/bash

set -e

parallel=$1
cd $(dirname "$0")

if [ -d /mnt/ramdisk ] ; then
	export HOME=/mnt/ramdisk
fi

mkdir -p reports

function versionInfo() {
	echo "Java Version"
	java -version
	echo "Maven Version"
	mvn -version
	echo "CPU Info"
	cat /prroc/cpuinfo
	echo "Memory Info"
	cat /proc/meminfo
}

versionInfo | tee -a reports/version.txt

./run.sh dl $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
#./run.sh $parallel

./stats.sh -f >reports/stats.txt
