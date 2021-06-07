#!/bin/bash

(
	if [ "$1" != "a"  ]; then
		git status reports |grep reports/run | tr -d " \t"
	else
		find reports/run*.txt
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

exit 0

echo 123 |tr -d '\000-\011\013\014\016-\037'| egrep -i \
	"(fail|BUILD SUCCESSFUL in|BUILD SUCCESS$|ELAPSED TIME)"|\
	egrep -v "(Failures: 0, Errors: 0|--fail-fast)" |\
	tr "\n" " "| \
	sed -e "s/BUILD SUCCESS.*ELAPSED TIME/ SUCCESS/;s/\[INFO\]//
	s/BUILD FAILURE.*ELAPSED TIME/FAILURE/;
	s/: .*BUILD FAILED in.*ELAPSED TIME/FAILURE/;
	s/[A-Z0-9]*FAILURE/FAILURE/;
	"|tr -s " "
