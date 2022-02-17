# Kafka Admin 
Simple REST based Kafka administration tool. It is not intended
for production use at the moment.

| Verb   | Endpoint                                | Usage                                                            |
|--------|-----------------------------------------|------------------------------------------------------------------|
| GET    | /topics                                 | curl -v http://localhost:8080/topics                             |
| GET    | /topics/describe                        | curl -v http://localhost:8080/topics/describe                    |
| GET    | /topic/describe/{name}                  | curl -v http://localhost:8080/topic/describe/my-topic            |
| POST   | /topic/{name}                           | curl -v -X POST http://localhost:8080/topic/my-topic             |
| POST   | /topic/{name}/partition/{count}         | curl -v -X POST http://localhost:8080/topic/my-topic/partition/2 |
| DELETE | /topic/{name}                           |                                                                  |
| DELETE | /topic/{name}/deleterecords/{partition} |                                                                  |
