# Kafka samples
## Docker

You can create Kafka cluster with docker-compose. Currently
it will create 1xzookeeper, 3xkafka and 1xcustom producer/consumer.

```
docker-compose up --build
```
If you need to start over
```
docker-compose rm
```

Kafka describe topic
```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic my-topic --zookeeper 172.18.101.2:2181
```

Zookeeper list broker ids
```
docker exec -it zookeeper /apache-zookeeper-3.6.2-bin/bin/zkCli.sh ls /brokers/ids
```
