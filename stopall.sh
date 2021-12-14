for in in {1..20} ; do 
	kill -9 $(ps auxwww | egrep "(java|run.sh)" |
		grep -v grep |tr -s " "|cut -f2 -d " ")
done