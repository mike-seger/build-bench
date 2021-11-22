cd $(dirname "$0")

sh ./run.sh dl
sh ./run.sh
sh ./run.sh
sh ./run.sh
sh ./run.sh
sh ./run.sh

sh stats.sh >reports/stats.txt
