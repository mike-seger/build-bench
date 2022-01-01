#!/bin/bash

set -e

parallel=$1
cd $(dirname "$0")

if [ -d /mnt/ramdisk ] ; then
	export HOME=/mnt/ramdisk
fi
mkdir -p reports

function versionInfo() {
	echo "# Java Version"
	java -version
	echo "# Maven Version"
	mvn -version
	echo "# CPU Info"
	[ -d /proc ] && (cat /proc/cpuinfo |grep "model name"|uniq) || (sysctl -a | grep machdep.cpu.brand_string)
	echo "# Memory Info"
	[ -d /proc ] && (cat /proc/meminfo|grep MemTotal) || (sysctl -a | grep hw.memsize)
	echo "# Disk Free"
	df -h .
	echo "#"
}

versionInfo | tee -a reports/version.txt

#./run.sh dl $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel

./stats.sh -f >reports/stats.txt
