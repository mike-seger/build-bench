#!/bin/bash

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
done
