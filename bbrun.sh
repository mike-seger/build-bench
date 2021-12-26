
cd $(dirname "$0")
./multirun.sh >/tmp/1 &
pid=$!
while [ 1==1 ] ; do
	ps aux | grep multirun.sh | egrep -v grep >/dev/null
	./watch-stats.sh
	sleep 10 
done