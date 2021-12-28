#!/bin/bash

while [ 1==1 ]; do 
	if [[ $# == 1 && ! -e /proc/$1/status ]]; then
	    break
	fi
	page=$(date; echo; ./stats.sh)
	clear
	printf "%s\n" "$page"
	sleep 10
done

echo "multirun finished"