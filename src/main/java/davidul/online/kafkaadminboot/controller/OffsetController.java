package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.exception.InternalException;
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
                                                 @PathVariable("partition") String partition){
        final ListOffsetsResult.ListOffsetsResultInfo earliestOffset;
        try {
            earliestOffset = this.topicService.offset(topicName, Integer.parseInt(partition), OffsetSpec.earliest());
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }

        final ListOffsetsResult.ListOffsetsResultInfo latestOffset;

        try {
            latestOffset = this.topicService.offset(topicName, Integer.parseInt(partition), OffsetSpec.latest());
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
        final FullOffsetDTO fullOffsetDTO = new FullOffsetDTO(earliestOffset.offset(), latestOffset.offset());
        return ResponseEntity.ok(fullOffsetDTO);
    }

    @GetMapping(value = "/topic/{name}/partition/{partition}/offset/{position}")
    public ResponseEntity<OffsetDTO> offset(@PathVariable("name") String topicName,
                                            @PathVariable("partition") String partition,
                                            @PathVariable("position") String position) {

        if (position.equalsIgnoreCase("earliest")) {
            final ListOffsetsResult.ListOffsetsResultInfo info;
            try {
                info = this.topicService.offset(topicName,
                        Integer.parseInt(partition),
                        OffsetSpec.earliest());
            } catch (InternalException e) {
                return ResponseEntity.internalServerError().build();
            }

            final OffsetDTO earliest = new OffsetDTO("earliest", info.offset());
            return ResponseEntity.ok(earliest);
        } else if (position.equalsIgnoreCase("latest")) {
            try {
                this.topicService.offset(topicName, Integer.parseInt(partition), OffsetSpec.latest());
            } catch (InternalException e) {
                return ResponseEntity.internalServerError().build();
            }
        }

        return ResponseEntity.accepted().build();
    }
}
