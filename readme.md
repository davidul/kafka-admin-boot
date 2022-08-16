# Kafka Admin 
Simple REST based Kafka administration tool. It is not intended
for production use at the moment.

Environment variable `KAFKA_BOOTSTRAP` defines the URL
of Kafka cluster. Pass it to docker run or docker-compose
command.

| Verb   | Endpoint                                | Usage                                                                  |
|--------|-----------------------------------------|------------------------------------------------------------------------|
| GET    | /topics                                 | curl -v http://localhost:8080/topics                                   |
| GET    | /topics/describe                        | curl -v http://localhost:8080/topics/describe                          |
| GET    | /topic/describe/{name}                  | curl -v http://localhost:8080/topic/describe/my-topic                  |
| POST   | /topic/{name}                           | curl -v -X POST http://localhost:8080/topic/my-topic                   |
| POST   | /topic/{name}/partition/{count}         | curl -v -X POST http://localhost:8080/topic/my-topic/partition/2       |
| DELETE | /topic/{name}                           | curl -v -X DELETE http://localhost:8080/topic/my-topic                 |
| DELETE | /topic/{name}/deleterecords/{partition} | curl -v -X DELETE http://localhost:8080/topic/my-topic/deleterecords/1 |

## Get Topics

`GET /topics` returns plain list of topic names

```json
[
    "another-topic",
    "my-topic"
]
```

## Describer topics

`GET /topics/describe`

Returns extended list of topics.

```json
[
    {
        "name": "my-topic",
        "partitions": [
            {
                "partition": 0,
                "leader": "172.19.0.3:9092 (id: 1001 rack: null)"
            }
        ]
    },
    {
        "name": "another-topic",
        "partitions": [
            {
                "partition": 0,
                "leader": "172.19.0.3:9092 (id: 1001 rack: null)"
            }
        ]
    }
]
```

Describe single topic

`GET /topic/describe/my-topic`

```json
{
    "name": "my-topic",
    "partitions": [
        {
            "partition": 0,
            "leader": "172.19.0.3:9092 (id: 1001 rack: null)"
        }
    ]
}
```

## Create topic

`POST /topic/another-topic`

Returns 202 Accepted in case of success.

## Create partition

`POST /topic/my-topic/partition/2`

Increase total partition count by `n`

