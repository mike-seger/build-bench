#!/bin/bash

(
	if [ "$1" == "r"  ]; then
		git status reports |grep reports/ | tr -d " \t"
	else
		find reports/*.txt
	fi
) | sort |while read f ; do 
	printf "%s: " "$f"
	tail -20 $f | egrep -i \
		"(fail|BUILD SUCCESSFUL in|^.INFO. BUILD SUCCESS$|ELAPSED TIME)"|\
		tr "\n" " "| sed -e "s/BUILD SUCCESS.*ELAPSED TIME/ SUCCESS/;s/\[INFO\]//
		s/BUILD FAILURE.*ELAPSED TIME/FAILURE/;
		s/: .*BUILD FAILED in.*ELAPSED TIME/FAILURE/;
		s/[A-Z0-9]*FAILURE/FAILURE/;
		"|tr -s " "
	echo
done