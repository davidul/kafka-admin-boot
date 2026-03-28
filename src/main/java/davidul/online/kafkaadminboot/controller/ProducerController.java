package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.model.RecordMetadataDTO;
import davidul.online.kafkaadminboot.service.ProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping(value = "/producer/{topic}", produces = "application/json")
    public ResponseEntity<RecordMetadataDTO> sendRecord(
            @RequestBody String message,
            @PathVariable("topic") String topic) {
        try {
            RecordMetadataDTO result = this.producerService.produce(topic, message);
            return ResponseEntity.status(HttpStatus.CREATED).body(result);
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
