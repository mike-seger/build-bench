FROM ubuntu:20.04
RUN apt update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends git openjdk-11-jdk-headless netcat iputils-ping maven curl wget


#cd /home/docker
#cp cacerts /usr/local/openjdk-11/lib/security/
#cp cacerts /opt/java/openjdk/lib/security/
#export MAVEM_OPTS="-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true"

