#!/bin/bash

gradle_depcacheargs=""
mvn_depcacheargs=""
dldeps=0
if [ "$1" == "dl" ] ; then
	gradle_depcacheargs=" --refresh-dependencies"
	mvn_depcacheargs=" -U"
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
	d=$(isodate)
	id=$(uname -prsm | tr "A-Z_ " a-z\-\-)
	f=../run_${id}_${project}_c${dldeps}_${d}.txt
	echo $d |tee -a $f
	echo $id |tee -a $f
	echo $project |tee -a $f
	
	if [ "$dldeps" == "1" ] ; then
		echo "Purging cache" |tee -a $f
		if [[ "$buildcmd" ==  *"./gradlew"* ]] ; then
			buildcmd="$buildcmd $gradle_depcacheargs"
			./gradlew --stop
			rm -fR .gradle/caches ~/.gradle/caches ~/.gradle/build-scan-data ~/.gradle/daemon \
				~/.gradle/workers ~/.gradle/wrapper  ~/.gradle/notifications 
		elif [[ "$buildcmd" ==  *"./mvnw"* ]] ; then
			buildcmd="$buildcmd $mvn_depcacheargs"
			./mvnw dependency:purge-local-repository
			rm -fR~/.m2/repository .m2/repository
		fi
	fi

	echo ${buildcmd} |tee -a $f
	STARTTIME=$(date +%s)
	${buildcmd} 2>&1|tee -a $f
	ENDTIME=$(date +%s)
	isodate >>$f
	echo "ELAPSED TIME: $(($ENDTIME - $STARTTIME)) s"|tee -a $f
	)
}

runIt junit5-r5.7.2 "./gradlew clean test build"
runIt spring-boot-2.4.6 "./gradlew clean test build"
exit 0

runIt maven-maven-3.8.1 "./mvnw -Drat.skip=true clean package"
runIt dropwizard-2.0.22 "./mvnw clean package"
runIt metrics-4.1.22 "./mvnw clean package"
runIt junit5-r5.7.2 "./gradlew clean test build"
runIt spring-data-jdbc-2.2.1 "./mvnw clean package"
runIt spring-boot-2.4.6 "./gradlew clean test build"
runIt spring-kafka-2.7.1 "./gradlew clean test build"
