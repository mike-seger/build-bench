#!/bin/bash

set -e

parallel=$1
cd $(dirname "$0")

mkdir -p reports

#./run.sh dl $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel
./run.sh $parallel

./stats.sh >reports/stats.txt
