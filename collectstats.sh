#!/bin/bash

reportsdir=$1
if [ ! -d "$reportsdir" ] ; then
	echo "Usage: $0 <reportsdir>"
	echo "<reportsdir> must contain report directories with a stats.txt in each"
	exit 1
fi

showtitle=1
for f in $(find "$reportsdir" -name stats.txt); do
	tim=$(cat $f | grep SUCCESS |\
		sed -e "s/run_//;s/^[^_]*_//;
			s/_.*.txt://;s/ SUCCESS : /\t/;s/ *s *$//"
		)
	categories=$(printf "%s" "$tim" | cut -f1|uniq)
	if [ $showtitle == 1 ] ; then
		echo -n "system"
		for c in $(echo $categories); do
			printf "\t%s" "$c"
		done
		showtitle=0
	fi
	echo
	echo -n "$f" | sed -e "s#/stats.txt##;s#.*/##"
	for c in $(echo $categories); do
		n=$(printf "%s" "$tim"|grep $c|\
			awk '{ sum += $2; n++ } END { if (n > 0) print sum / n; }'|\
			awk '{print int($1+0.5)}')
		printf "\t%s" "$n"
		#cut -f2|sort -n|head -3 
	done
	echo 
#	sed -e "s/run_//;s/^[^_]*_//;s/_.*.txt://"
	
done
