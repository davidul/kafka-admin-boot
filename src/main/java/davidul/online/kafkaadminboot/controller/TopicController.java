package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.model.ClusterDTO;
import davidul.online.kafkaadminboot.model.TopicPartitionsDTO;
import davidul.online.kafkaadminboot.model.OffsetDTO;
import davidul.online.kafkaadminboot.service.TopicService;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
public class TopicController {

    private final TopicService topicService;

    public TopicController(TopicService topicService) {
        this.topicService = topicService;
    }

    @GetMapping(value = "/topics", params = {"internal"}, produces = "application/json")
    public ResponseEntity<Set<String>> getTopics(@RequestParam("internal") String internal) {
        Boolean listInternal = Boolean.FALSE;
        if(internal != null){
            listInternal = Boolean.valueOf(internal);
        }
        final Set<String> strings = this.topicService.listTopics(listInternal);
        return ResponseEntity.ok(strings);
    }

    @GetMapping(value = "/topics/describe", produces = "application/json")
    public ResponseEntity<List<TopicPartitionsDTO>> describeTopics(@RequestParam("internal") String internal) {
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
    public ResponseEntity<TopicPartitionsDTO> describeTopic(@PathVariable("name") String name) {
        final TopicDescription topicDescription = this.topicService.describeTopic(name);
        final TopicPartitionsDTO topicPartitionsDTO = new TopicPartitionsDTO(topicDescription.name(), Topics.partitions(topicDescription.partitions()));
        return ResponseEntity.ok(topicPartitionsDTO);
    }

    @PostMapping(value = "/topic/{name}")
    public ResponseEntity<Void> createTopic(@PathVariable("name") String name) {
        this.topicService.createTopic(name);
        return ResponseEntity.accepted().build();
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
    public ResponseEntity<Void> deleteRecords(@PathVariable("name") String topicName, @PathVariable("partition") String partition) {
        this.topicService.deleteRecords(topicName, Integer.parseInt(partition));
        return ResponseEntity.accepted().build();
    }





    @GetMapping(value = "/cluster")
    public ResponseEntity<ClusterDTO> getCluster(){
        final ClusterDTO clusterDTO = this.topicService.describeCluster();
        return ResponseEntity.ok(clusterDTO);
    }
}
