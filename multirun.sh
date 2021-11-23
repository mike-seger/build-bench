cd $(dirname "$0")

./run.sh dl
./run.sh
./run.sh
./run.sh
./run.sh
./run.sh

./stats.sh >reports/stats.txt
