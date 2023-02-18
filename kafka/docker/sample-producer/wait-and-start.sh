#!/bin/bash


set -m
set -e

count=10
i=1

log() {
  echo "[$i] [$(date +"%T")] $@"
  i=`expr $i + 1`
}

for k in {1..20}
do
  log "Waitig $k"
  sleep 1
  if [ $k = 20 ]; then
    java -jar /usr/verticles/kafka-samples-1.0-SNAPSHOT-fat.jar
  fi
done

#-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=7896
