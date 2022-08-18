package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.ClusterDTO;
import davidul.online.kafkaadminboot.model.FutureDTO;
import davidul.online.kafkaadminboot.model.TopicPartitionsDTO;
import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import davidul.online.kafkaadminboot.model.internal.ListTopicsDTO;
import davidul.online.kafkaadminboot.service.KafkaResultQueue;
import davidul.online.kafkaadminboot.service.TopicService;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

@RestController
public class TopicController {

    private final TopicService topicService;

    private final KafkaResultQueue resultQueue;

    private Logger logger = LoggerFactory.getLogger(TopicController.class);

    public TopicController(TopicService topicService, KafkaResultQueue resultQueue) {
        this.topicService = topicService;
        this.resultQueue = resultQueue;
    }

    @GetMapping(value = "/topics", produces = "application/json")
    public ResponseEntity<Set<String>> getTopics(@RequestParam(value = "internal", required = false) String internal,
                                                 @RequestHeader(name = "queue-id", required = false) String queueId) {

        logger.debug("header key {}", queueId);
        if(queueId == null) {
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

            return ResponseEntity.ok(listTopicsDTO.getTopicNames());
        }else{
            KafkaRequest kafkaRequest = resultQueue.get(queueId);
            if(kafkaRequest.isDone()) {
                try {
                    Object o = kafkaRequest.getKafkaFuture().get();
                    logger.debug("Kafka topics {}", o);
                    logger.debug("Set of {}", o);
                    return ResponseEntity.ok((Set<String>) o);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return ResponseEntity.notFound().build();
    }

    @GetMapping(value = "/topics/describe", produces = "application/json")
    public ResponseEntity<List<TopicPartitionsDTO>> describeTopics(@RequestParam(value = "internal", required = false) String internal) {
        Boolean isInternal = Boolean.FALSE;
        if(internal != null){
            isInternal = Boolean.parseBoolean(internal);
        }
        List<TopicPartitionsDTO> topicList = new ArrayList<>();
        final Map<String, TopicDescription> topicsAll = topicService.describeTopicsAll(isInternal);
        final Iterator<String> iterator = topicsAll.keySet().iterator();
        while (iterator.hasNext()) {
            final String next = iterator.next();
            final TopicDescription topicDescription = topicsAll.get(next);
            final TopicPartitionsDTO topicPartitionsDTO = new TopicPartitionsDTO(next, Topics.partitions(topicDescription.partitions()));
            topicList.add(topicPartitionsDTO);
        }

        return ResponseEntity.ok(topicList);
    }

    @GetMapping(value = "/topic/describe/{name}", produces = "application/json")
    public ResponseEntity<TopicPartitionsDTO> describeTopic(@PathVariable("name") String name,
                                                            @RequestHeader(name = "queue-id", required = false) String queueId) {
        if(queueId == null) {
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
        }else{
            KafkaRequest kafkaRequest = resultQueue.get(queueId);
            if(kafkaRequest.isDone()) {
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

    @PostMapping(value = "/topic/{name}")
    public ResponseEntity<FutureDTO> createTopic(@PathVariable("name") String name) {
        String uuid = null;
        try {
            uuid = this.topicService.createTopic(name);
        } catch (InternalException e) {
            throw new RuntimeException(e);
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        }
        return ResponseEntity.accepted().body(new FutureDTO(uuid));
        //return ResponseEntity.accepted().build();
    }

    @PostMapping(value = "/topic/{name}/partition/{count}")
    public ResponseEntity<Void> addPartition(@PathVariable("name") String name, @PathVariable("count") Integer count) {
        this.topicService.createPartition(name, count);
        return ResponseEntity.accepted().build();
    }


    @DeleteMapping(value = "/topic/{name}")
    public ResponseEntity<Void> deleteTopic(@PathVariable("name") String name) {
        this.topicService.deleteTopic(name);
        return ResponseEntity.accepted().build();
    }

    @PostMapping(value = "/topic/{name}/deleterecords/{partition}")
    public ResponseEntity<Void> deleteRecords(@PathVariable("name") String topicName,
                                              @PathVariable("partition") String partition) {
        try {
            this.topicService.deleteRecords(topicName, Integer.parseInt(partition));
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
        return ResponseEntity.accepted().build();
    }


    @GetMapping(value = "/cluster")
    public ResponseEntity<ClusterDTO> getCluster(){
        final ClusterDTO clusterDTO = this.topicService.describeCluster();
        return ResponseEntity.ok(clusterDTO);
    }
}
