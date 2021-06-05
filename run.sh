#!/bin/bash

dldeps=0
if [ "$1" == "dl" ] ; then
	dldeps=1
fi

function isodate() {
	date -u +"%Y-%m-%dT%H:%M:%SZ" | tr -d ":-"|sed -e "s/[+Z].*//"
}

function runIt() {
	local project=$1
	local workdir=$(dirname "$0")/$project
	local buildcmd=$2
	
	(
	cd "${workdir}"
	git checkout . # restore original source because of rogue build projects like spring-boot
	git clean -fd
	d=$(isodate)
	id=$(uname -prsm | tr "A-Z_ " a-z\-\-)
	f=../reports/run_${id}_${project}_c${dldeps}_${d}.txt
	echo $d |tee -a $f
	echo $id |tee -a $f
	echo $project |tee -a $f
	
	gcacheopts="--project-cache-dir=$(pwd)/gradlecache"
	if [ "$dldeps" == "1" ] ; then
		echo "Purging cache" |tee -a $f
		if [[ "$buildcmd" == *"./gradlew"* ]] ; then
			buildcmd="$buildcmd --refresh-dependencies $gcacheopts"
			buildcmd="${buildcmd/ test / test --fail-fast }"
			./gradlew --status $gcacheopts --refresh-dependencies |\
				 grep -v PID|sed -e "s/^ *//;s/ .*//"|grep "^[0-9]" |\
				 xargs kill -9 >/dev/null 2>&1
		elif [[ "$buildcmd" ==  *"./mvnw"* ]] ; then
			buildcmd="$buildcmd -U --fail-fast -Dsurefire.skipAfterFailureCount=1"
			./mvnw dependency:purge-local-repository
			rm -fR ~/.m2/repository .m2/repository
		fi
	else
		if [[ "$buildcmd" == *"./gradlew"* ]] ; then
			buildcmd="$buildcmd $gcacheopts"
			buildcmd="${buildcmd/ test / test --fail-fast }"
		elif [[ "$buildcmd" ==  *"./mvnw"* ]] ; then
			buildcmd="$buildcmd -fail-fast -Dsurefire.skipAfterFailureCount=1"
		fi
	fi

	echo ${buildcmd} |tee -a $f
	STARTTIME=$(date +%s)
	eval "${buildcmd}" 2>&1|tee -a $f
	ENDTIME=$(date +%s)
	isodate >>$f
	echo "ELAPSED TIME: $(($ENDTIME - $STARTTIME)) s"|tee -a $f
	)
}

#runIt junit5-r5.7.2 "./gradlew clean"
runIt spring-boot-2.4.6 "./gradlew clean"
#runIt junit5-r5.7.2 "./gradlew clean test build"
#runIt spring-boot-2.4.6 "./gradlew clean test build"
exit 0

runIt maven-maven-3.8.1 "./mvnw -Drat.skip=true clean package"
runIt dropwizard-2.0.22 "./mvnw clean package"
runIt metrics-4.1.22 "./mvnw clean package"
runIt junit5-r5.7.2 "./gradlew clean test build"
runIt spring-data-jdbc-2.2.1 "./mvnw clean package"
runIt spring-boot-2.4.6 "./gradlew clean test build"
runIt spring-kafka-2.7.1 "./gradlew clean test build"
