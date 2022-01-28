# Kafka samples
## Docker

You can create Kafka cluster with docker-compose. It will
create zookeeper and kafka container.
Kafka broker id is random. Should not be in collision.

```
docker-compose up --build
```
If you need to start over
```
docker-compose rm
```

Scaling up with
```shell
docker-compose up --scale kafka=3
```
will create three kafka brokers

Kafka describe topic
```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --describe --topic my-topic --zookeeper 172.18.101.2:2181
```

Zookeeper list broker ids
```
docker exec -it zookeeper zkCli.sh ls /brokers/ids
```
docker exec docker-kafka-1 less /opt/kafka/config/server.properties
