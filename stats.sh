find reports/*.txt | sort |while read f ; do 
	echo -n $f; tail -20 $f | egrep -i \
		"(fail|BUILD SUCCESSFUL in|^.INFO. BUILD SUCCESS$|ELAPSED TIME)"|\
		tr "\n" " "| sed -e "s/BUILD SUCCESS.*ELAPSED TIME/ SUCCESS/;s/\[INFO\]//"|tr -s " "
	echo
done