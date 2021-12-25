#!/bin/bash

export full=0
if [ "$1" == "-f" ]; then
	export full=1
	shift
fi

(
	
	if [ "$1" == "n"  ]; then
		git status shared-reports |grep shared-reports/run | tr -d " \t"
	elif [ "$1" == "o"  ]; then
		find shared-reports -name "run*.txt"
	else
		find reports -name "run*.txt"
	fi
) | sort |while read f ; do 
	printf "%s: " "$f"
	tail -20 $f | \
		tr -d '\000-\011\013\014\016-\037' |\
		egrep "(BUILD.*(SUCCESS|FAIL)|ELAPSED)" |\
		sed -E "s/.*(BUILD )//;s/(SUCCESS|FAILED|FAILURE).*/\1/" |\
		sed -e "s/ELAPSED TIME//"| tr "\n" " "|tr -s " "
	echo
done | (
	if [ "$full" == "0" ] ; then
		tr "_" "\t" |cut -f3,5- |sed -e "s/..\.txt://" | tr -s " "
	else
		grep ".*"
	fi
)
