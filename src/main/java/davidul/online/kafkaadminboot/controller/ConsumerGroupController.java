package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.ConsumerGroupDescriptionDTO;
import davidul.online.kafkaadminboot.model.ConsumerGroupListingDTO;
import davidul.online.kafkaadminboot.model.ConsumerGroupOffsetDTO;
import davidul.online.kafkaadminboot.model.OffsetAndMetadataDTO;
import davidul.online.kafkaadminboot.model.TopicPartitionDTO;
import davidul.online.kafkaadminboot.service.TopicService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
public class ConsumerGroupController {

    private final TopicService topicService;

    public ConsumerGroupController(TopicService topicService) {
        this.topicService = topicService;
    }

    @GetMapping(value = "/consumergroups")
    public ResponseEntity<List<ConsumerGroupListingDTO>> listConsumerGroups() {
        try {
            return ResponseEntity.ok(this.topicService.listConsumerGroups());
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping(value = "/consumergroup/{groupId}/offset")
    public ResponseEntity<List<ConsumerGroupOffsetDTO>> listConsumerGroupOffsets(
            @PathVariable("groupId") String groupId) {
        final Map<TopicPartitionDTO, OffsetAndMetadataDTO> map;
        try {
            map = this.topicService.listConsumerGroupOffsets(groupId);
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }

        List<ConsumerGroupOffsetDTO> offsetDTOS = new ArrayList<>();
        for (Map.Entry<TopicPartitionDTO, OffsetAndMetadataDTO> entry : map.entrySet()) {
            offsetDTOS.add(new ConsumerGroupOffsetDTO(entry.getKey(), entry.getValue()));
        }
        return ResponseEntity.ok(offsetDTOS);
    }

    @GetMapping(value = "/consumergroup/{groupId}/describe")
    public ResponseEntity<Map<String, ConsumerGroupDescriptionDTO>> describerConsumerGroup(
            @PathVariable("groupId") String groupId) {
        try {
            return ResponseEntity.ok(
                    this.topicService.describerConsumerGroups(Collections.singleton(groupId)));
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
