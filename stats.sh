find reports/*c1* | while read f ; do 
	echo -n $f; tail -20 $f | egrep -i \
		"(fail|BUILD SUCCESSFUL in|^.INFO. BUILD SUCCESS$)"|\
		tr "\n" " "
	echo
done