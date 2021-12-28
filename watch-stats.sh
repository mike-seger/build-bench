while [ 1==1 ]; do 
	ps auxwww | grep multirun.sh >/dev/null || break
	page=$(date; echo./stats.sh;)
	clear
	printf "%s\n" "$page"
	sleep 10
done

echo "multirun finished"