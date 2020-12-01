#!/bin/bash
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic --partitions 3 --replication-factor 3 --config max.message.bytes=64000 --config flush.messages=1