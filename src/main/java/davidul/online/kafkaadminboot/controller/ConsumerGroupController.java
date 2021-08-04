package davidul.online.kafkaadminboot.controller;

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
import java.util.List;
import java.util.Map;

@RestController
public class ConsumerGroupController {

    private final TopicService topicService;

    public ConsumerGroupController(TopicService topicService) {
        this.topicService = topicService;
    }

    @GetMapping(value = "/consumergroups")
    public ResponseEntity<List<ConsumerGroupListingDTO>> listConsumerGroups(){
        final List<ConsumerGroupListingDTO> consumerGroupListingDTOS = this.topicService.listConsumerGroups();
        return ResponseEntity.ok(consumerGroupListingDTOS);
    }

    @GetMapping(value = "/consumergroup/{groupId}/offset")
    public ResponseEntity<List<ConsumerGroupOffsetDTO>> listConsumerGroupOffsets(@PathVariable("groupId") String groupId){
        List<ConsumerGroupOffsetDTO> offsetDTOS = new ArrayList<>();
        final Map<TopicPartitionDTO, OffsetAndMetadataDTO> map = this.topicService.listConsumerGroupOffsets(groupId);
        for (TopicPartitionDTO topicPartitionDTO : map.keySet()) {
            final ConsumerGroupOffsetDTO consumerGroupOffsetDTO = new ConsumerGroupOffsetDTO(topicPartitionDTO, map.get(topicPartitionDTO));
            offsetDTOS.add(consumerGroupOffsetDTO);
        }

        return ResponseEntity.ok(offsetDTOS);
    }
}
