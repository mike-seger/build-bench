while [ 1==1 ]; do page=$(date; echo; ./stats.sh;); clear; printf "%s\n" "$page";  sleep 10; done
