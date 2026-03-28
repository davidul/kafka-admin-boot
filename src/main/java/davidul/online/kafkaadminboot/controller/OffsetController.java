package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.FullOffsetDTO;
import davidul.online.kafkaadminboot.model.OffsetDTO;
import davidul.online.kafkaadminboot.service.TopicService;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OffsetController {

    private final TopicService topicService;

    public OffsetController(TopicService topicService) {
        this.topicService = topicService;
    }

    @GetMapping(value = "/topic/{name}/partition/{partition}/offset")
    public ResponseEntity<FullOffsetDTO> offsets(@PathVariable("name") String topicName,
                                                 @PathVariable("partition") String partition) {
        try {
            ListOffsetsResult.ListOffsetsResultInfo earliest =
                    topicService.offset(topicName, Integer.parseInt(partition), OffsetSpec.earliest());
            ListOffsetsResult.ListOffsetsResultInfo latest =
                    topicService.offset(topicName, Integer.parseInt(partition), OffsetSpec.latest());
            return ResponseEntity.ok(new FullOffsetDTO(earliest.offset(), latest.offset()));
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping(value = "/topic/{name}/partition/{partition}/offset/{position}")
    public ResponseEntity<OffsetDTO> offset(@PathVariable("name") String topicName,
                                            @PathVariable("partition") String partition,
                                            @PathVariable("position") String position) {
        OffsetSpec spec = position.equalsIgnoreCase("earliest")
                ? OffsetSpec.earliest()
                : OffsetSpec.latest();
        try {
            ListOffsetsResult.ListOffsetsResultInfo info =
                    topicService.offset(topicName, Integer.parseInt(partition), spec);
            return ResponseEntity.ok(new OffsetDTO(position.toLowerCase(), info.offset()));
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
