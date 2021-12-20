#!/bin/bash

dldeps=0
#export maven=./mvnw
export maven=mvn

if [ "$1" == "dl" ] ; then
	dldeps=1
	shift
fi

if [ "$1" == "p" ] ; then
	parallel=1
	shift
fi

function isodate() {
	date -u +"%Y-%m-%dT%H:%M:%SZ" | tr -d ":-"|sed -e "s/[+Z].*//"
}

function runIt() {
	local project=$1
	local workdir=$(dirname "$0")/$project
	local buildcmd=$2
	local gb_opts=""
	sleep 1
	(
	if [[ "$OS" == *Windows*  ]] ; then
		if [[ -x $(which cygpath) && "$JAVA_HOME" == /cygdrive/*  ]] ; then
			export JAVA_HOME=$(cygpath -w $JAVA_HOME)
		fi
		#maven="./mvnw.cmd"
		#buildcmd="${buildcmd/mvnw/mvnw.cmd}"
	fi
	if [[ -z "$JAVA_HOME" ]] ; then
		export JAVA_HOME=$(cd $(dirname "$(which java)")/.. >/dev/null; pwd)
	fi
	cd "${workdir}"
	git checkout . # restore original source because of rogue build projects like spring-boot
	git clean -fd
	d=$(isodate)
	id=$(uname -prsm | tr "A-Z_ " a-z\-\-| tr -d "()/" | sed -e "s/-x86-64-unknown//")
	f=../reports/run_${id}_${project}_${d}_c${dldeps}.txt
	echo $d |tee -a $f
	echo $id |tee -a $f
	echo $project |tee -a $f
	gcacheopts="--project-cache-dir=$(pwd)/gradlecache"
	if [ "$dldeps" == "1" ] ; then
		echo "Purging cache" |tee -a $f
		if [[ "$buildcmd" == *"./gradlew"* ]] ; then
			export GRADLE_OPTS="$GRADLE_OPTS -Dorg.gradle.daemon=false"
		
			buildcmd="$buildcmd "
			buildcmd="${buildcmd/ test / test --fail-fast }"
			[ -f doformat.txt ] && buildcmd="${buildcmd/ build / build -x checkstyleMain }"
#			buildcmd="${buildcmd/ build / build $gb_opts -x checkstyleNohttp -x checkstyleMain -x checkstyleTest --refresh-dependencies --no-daemon }"
			buildcmd="${buildcmd/ build / build $gb_opts --refresh-dependencies --no-daemon }"
			buildcmd="$buildcmd $gcacheopts"
			./gradlew $gcacheopts --stop
			if [[ "$OS" == *Windows*  ]] ; then
				taskkill /IM java.exe /F
			else
				./gradlew --status $gcacheopts |\
					grep -v PID|sed -e "s/^ *//;s/ .*//"|grep "^[0-9]" |\
					xargs kill -9 >/dev/null 2>&1
			fi
			rm -fR ~/.gradle/cache ~/.gradle/wrapper .gradle/cache gradlecache 
		elif [[ "$buildcmd" ==  *"mvn"* ]] ; then
			buildcmd="$buildcmd -U --fail-fast -Dsurefire.skipAfterFailureCount=1"
			$maven dependency:purge-local-repository
			rm -fR ~/.m2/repository .m2/repository
		fi
	else
		if [[ "$buildcmd" == *"./gradlew"* ]] ; then
			buildcmd="$buildcmd $gcacheopts"
			[ -f doformat.txt ] && ./gradlew -p buildSrc format && ./gradlew format
#			buildcmd="${buildcmd/ build / build $gb_opts -x checkstyleNohttp -x checkstyleMain -x checkstyleTest }"
			buildcmd="${buildcmd/ build / build $gb_opts }"
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

	git checkout . # restore original source because of rogue build projects like spring-boot
	git clean -fd
	)
}

MY_OPTS="-T 8"

function runMavenMaven() {
	runIt maven-maven-3.8.1 "$maven $MY_OPTS -Drat.skip=true clean test package"
}

function runDropWizard() { 
	runIt dropwizard-2.0.22 "$maven $MY_OPTS clean test package"
}

function runMetrics() {
	runIt metrics-4.1.22 "$maven clean test package"
}

function runSpringData() {
	runIt spring-data-jdbc-2.2.1 "$maven clean test package"
}

function runSingle() {
	runMavenMaven
	runDropWizard
	runMetrics
	runSpringData
}

function runParallel() {
	(for i in {1..8} ; do runMavenMaven || break; done) &
	(for i in {1..1} ; do runDropWizard || break; done) &
	(for i in {1..2} ; do runMetrics || break; done) &
	(for i in {1..10} ; do runSpringData || break; done) &
}

if [ "$parallel" != "1" ] ; then
	runSingle
else
	runParallel
	wait
fi

wait

#runIt testng-7.4.0 "./gradlew clean test build"
#runIt micronaut-core-2.5.5-master "./gradlew clean test build"
#runIt spring-boot-2.4.6 "./gradlew clean test build"
#runIt spring-kafka-2.7.1 "./gradlew clean test build"

# fix old names
# rename "s/_(c[01])_(.*).txt/_\2_\1_.txt/" *.txt

# Count DL
# ls reports/run* | while read f ; do echo -n "$f: " ; grep -i download "$f"| wc -l; done