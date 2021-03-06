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
	local gb_opts=""
        local maven="./mvnw"
	(
	if [[ "$OS" == *Windows*  ]] ; then
		export JAVA_HOME=$(cygpath -w $JAVA_HOME)
		maven="./mvnw.cmd"
		buildcmd="${buildcmd/mvnw/mvnw.cmd}"
	fi
	cd "${workdir}"
	git checkout . # restore original source because of rogue build projects like spring-boot
	git clean -fd
	d=$(isodate)
	id=$(uname -prsm | tr "A-Z_ " a-z\-\-| tr -d "()/" | sed -e "s/-x86-64-unknown//")
	f=../reports/run_${id}_${project}_c${dldeps}_${d}.txt
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
		elif [[ "$buildcmd" ==  *"./mvnw"* ]] ; then
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

runIt maven-maven-3.8.1 "./mvnw -Drat.skip=true clean test package"
runIt dropwizard-2.0.22 "./mvnw clean test package"
runIt metrics-4.1.22 "./mvnw clean test package"
runIt spring-data-jdbc-2.2.1 "./mvnw clean test package"
runIt testng-7.4.0 "./gradlew clean test build"
runIt micronaut-core-2.5.5-master "./gradlew clean test build"
runIt spring-boot-2.4.6 "./gradlew clean test build"
runIt spring-kafka-2.7.1 "./gradlew clean test build"
