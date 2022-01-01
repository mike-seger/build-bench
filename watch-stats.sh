#!/bin/bash

while [ 1==1 ]; do 
	if [ -d /proc ] ; then
		if [[ $# == 1 && ! -e /proc/$1/status ]]; then
		    break
		fi
	else
		ps -p $1 > /dev/null 2>&1
		if [ $? != 0 ] ; then
			break
		fi
	fi
	page=$(date; echo; ./stats.sh)
	clear
	printf "%s\n" "$page"
	sleep 10
done

echo "multirun finished"
