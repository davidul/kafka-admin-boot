#!/bin/sh

#ipaddress=$(eval "route -n | awk '/UG[ \t]/{print $$2}'")
ipaddress=$(eval "ifconfig eth0 | grep -w 'inet' | tr -s ' ' | cut -d' ' -f3| cut -d':' -f2")
#092/tcp -> 0.0.0.0:50830
dockerport=$(docker port ${HOSTNAME} | cut -d":" -f2)

echo $dockerport
echo $ZOOKEEPER

cat <<EOF > $KAFKA_HOME/config/server.properties
broker.id.generation.enable=true
#control.plane.listener.name=INTERNAL
listeners=INTERNAL://${ipaddress}:9092,EXTERNAL://${ipaddress}:19092
advertised.listeners=INTERNAL://${ipaddress}:9092,EXTERNAL://${ADVERTISED_LISTENER}:${dockerport}
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400

socket.request.max.bytes=104857600
log.dirs=/tmp/kafka-logs
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

log.retention.hours=168

log.segment.bytes=1073741824

log.retention.check.interval.ms=300000

zookeeper.connect=$ZOOKEEPER

zookeeper.connection.timeout.ms=18000

group.initial.rebalance.delay.ms=0

EOF

echo $ipaddress


$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

#$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic --partitions 3 --replication-factor 2 --config max.message.bytes=64000 --config flush.messages=1