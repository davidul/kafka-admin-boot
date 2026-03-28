package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.FutureDTO;
import davidul.online.kafkaadminboot.model.TopicDTO;
import davidul.online.kafkaadminboot.model.TopicPartitionsDTO;
import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import davidul.online.kafkaadminboot.model.internal.ListTopicsDTO;
import davidul.online.kafkaadminboot.service.KafkaResultQueue;
import davidul.online.kafkaadminboot.service.TopicService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@RestController
public class TopicController {

    private final TopicService topicService;

    private final KafkaResultQueue resultQueue;

    private final Logger logger = LoggerFactory.getLogger(TopicController.class);

    public TopicController(TopicService topicService, KafkaResultQueue resultQueue) {
        this.topicService = topicService;
        this.resultQueue = resultQueue;
    }

    /**
     *
     * @param internal includes internal topics in response
     * @param queueId  retrieve the response from queue
     * @return list of topics
     */
    @Operation(summary = "Get list of topics")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the topics",
                    content = { @Content(mediaType = "application/json",
                            schema = @Schema(implementation = TopicDTO.class)) }),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content),
            @ApiResponse(responseCode = "202", description = "Request accepted",
                    content = @Content)
    })
    @GetMapping(value = "/topics", produces = "application/json")
    public ResponseEntity<List<TopicDTO>> getTopics(@RequestParam(value = "internal", required = false) String internal,
                                                    @RequestHeader(name = "queue-id", required = false) String queueId) {

        logger.debug("header key {}", queueId);
        if (queueId == null) {
            Boolean listInternal = Boolean.FALSE;
            if (internal != null) {
                listInternal = Boolean.valueOf(internal);
            }
            ListTopicsDTO listTopicsDTO;
            try {
                listTopicsDTO = this.topicService.listTopics(listInternal);
            } catch (InternalException e) {
                return ResponseEntity.internalServerError().build();
            } catch (KafkaTimeoutException e) {
                return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
            }

            List<TopicDTO> topics = listTopicsDTO
                    .getTopicNames()
                    .stream()
                    .map(TopicDTO::new)
                    .collect(Collectors.toList());
            return ResponseEntity.ok(topics);
        } else {
            KafkaRequest kafkaRequest = resultQueue.get(queueId);
            if (kafkaRequest != null && kafkaRequest.isDone()) {
                try {
                    Set<String> o = (Set<String>)kafkaRequest.getKafkaFuture().get();
                    logger.debug("Kafka topics {}", o);
                    logger.debug("Set of {}", o);
                    List<TopicDTO> collect = o.stream().map(t -> new TopicDTO(t)).collect(Collectors.toList());
                    return ResponseEntity.ok(collect);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return ResponseEntity.notFound().build();
    }

    /**
     * @param internal includes internal topics in response
     * @param queueId retrieve the response from queue
     * @return list of topics with partitions
     */
    @Operation(summary = "Describe all topics")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the topics",
                    content = { @Content(mediaType = "application/json",
                            schema = @Schema(implementation = TopicPartitionsDTO.class)) }),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content),
            @ApiResponse(responseCode = "202", description = "Request accepted",
                    content = @Content)
    })
    @GetMapping(value = "/topics/describe", produces = "application/json")
    public ResponseEntity<List<TopicPartitionsDTO>> describeTopics(@RequestParam(value = "internal", required = false)
                                                                   String internal,
                                                                   @RequestHeader(name = "queue-id", required = false)
                                                                   String queueId) {
        Boolean isInternal = Boolean.FALSE;
        if (queueId == null) {
            if (internal != null) {
                isInternal = Boolean.parseBoolean(internal);
            }
            List<TopicPartitionsDTO> topicList = new ArrayList<>();
            final Map<String, TopicDescription> topicsAll;
            try {
                topicsAll = topicService.describeTopicsAll(isInternal);
            } catch (KafkaTimeoutException e) {
                return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
            } catch (InternalException e) {
                return ResponseEntity.internalServerError().build();
            }

            for (String next : topicsAll.keySet()) {
                final TopicDescription topicDescription = topicsAll.get(next);
                final TopicPartitionsDTO topicPartitionsDTO = new TopicPartitionsDTO(next, Topics.partitions(topicDescription.partitions()));
                topicList.add(topicPartitionsDTO);
            }

            return ResponseEntity.ok(topicList);
        } else {
            KafkaRequest kafkaRequest = resultQueue.get(queueId);
            if (kafkaRequest != null && kafkaRequest.isDone()) {
                try {
                    Object o = kafkaRequest.getKafkaFuture().get();
                    logger.debug("Kafka topics {}", o);
                    logger.debug("Set of {}", o);
                    return ResponseEntity.ok((List<TopicPartitionsDTO>) o);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return ResponseEntity.notFound().build();
    }

    @Operation(summary = "Describe a specific topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Found the topic",
                    content = { @Content(mediaType = "application/json",
                            schema = @Schema(implementation = TopicPartitionsDTO.class)) }),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content),
            @ApiResponse(responseCode = "202", description = "Request accepted",
                    content = @Content)
    })
    @GetMapping(value = "/topic/describe/{name}", produces = "application/json")
    public ResponseEntity<TopicPartitionsDTO> describeTopic(@PathVariable("name") String name,
                                                            @RequestHeader(name = "queue-id", required = false) String queueId) {
        if (queueId == null) {
            final TopicDescription topicDescription;
            try {
                topicDescription = this.topicService.describeTopic(name);
                final TopicPartitionsDTO topicPartitionsDTO = new TopicPartitionsDTO(topicDescription.name(),
                        Topics.partitions(topicDescription.partitions()));
                return ResponseEntity.ok(topicPartitionsDTO);
            } catch (InternalException e) {
                return ResponseEntity.internalServerError().build();
            } catch (KafkaTimeoutException e) {
                return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
            }
        } else {
            KafkaRequest kafkaRequest = resultQueue.get(queueId);
            if (kafkaRequest.isDone()) {
                try {
                    Object o = kafkaRequest.getKafkaFuture().get();
                    logger.debug("Kafka topics {}", o);
                    logger.debug("Set of {}", o);
                    TopicDescription d = (TopicDescription) o;
                    return ResponseEntity.ok(new TopicPartitionsDTO(d.name(), Topics.partitions(d.partitions())));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return ResponseEntity.notFound().build();
    }

    @Operation(summary = "Create a new topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Topic creation accepted",
                    content = { @Content(mediaType = "application/json",
                            schema = @Schema(implementation = FutureDTO.class)) }),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content)
    })
    @PostMapping(value = "/topic/{name}")
    public ResponseEntity<FutureDTO> createTopic(@PathVariable("name") String name,
                                                 @RequestHeader(name = "queue-id", required = false) String queueId) {

        if (queueId == null) {
            String uuid = null;
            try {
                uuid = this.topicService.createTopic(name);
            } catch (InternalException e) {
                return ResponseEntity.internalServerError().build();
            } catch (KafkaTimeoutException e) {
                return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
            }
            return ResponseEntity.accepted().body(new FutureDTO(uuid));
        } else {
            KafkaRequest kafkaRequest = resultQueue.get(queueId);
            if (kafkaRequest != null && kafkaRequest.isDone()) {
                try {
                    Object o = kafkaRequest.getKafkaFuture().get();
                    logger.debug("Kafka topics {}", o);
                    logger.debug("Set of {}", o);
                    return ResponseEntity.ok((FutureDTO) o);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return ResponseEntity.notFound().build();
    }

    @Operation(summary = "Add partitions to a topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Partition addition accepted",
                    content = @Content)
    })
    @PostMapping(value = "/topic/{name}/partition/{count}")
    public ResponseEntity<Void> addPartition(@PathVariable("name") String name, @PathVariable("count") Integer count) {
        try {
            this.topicService.createPartition(name, count);
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
        return ResponseEntity.accepted().build();
    }


    @Operation(summary = "Delete a topic")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Topic deletion accepted",
                    content = @Content),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content)
    })
    @DeleteMapping(value = "/topic/{name}")
    public ResponseEntity<Void> deleteTopic(@PathVariable("name") String name) {
        try {
            this.topicService.deleteTopic(name);
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        } catch (InternalException e) {
            throw new RuntimeException(e);
        }
        return ResponseEntity.accepted().build();
    }

    @Operation(summary = "Delete records from a topic partition")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Record deletion accepted",
                    content = @Content),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content)
    })
    @PostMapping(value = "/topic/{name}/deleterecords/{partition}")
    public ResponseEntity<Void> deleteRecords(@PathVariable("name") String topicName,
                                              @PathVariable("partition") String partition) {
        try {
            this.topicService.deleteRecords(topicName, Integer.parseInt(partition));
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
        return ResponseEntity.accepted().build();
    }


}
