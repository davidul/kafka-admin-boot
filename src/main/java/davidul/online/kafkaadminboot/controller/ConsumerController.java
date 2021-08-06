package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.model.MessageDTO;
import davidul.online.kafkaadminboot.service.ConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class ConsumerController {

    private final ConsumerService consumerService;

    public ConsumerController(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }


    @GetMapping(value = "/consumer/{topic}", produces = "application/json")
    public ResponseEntity<List<MessageDTO>> consumer(@PathVariable("topic") String topic){
        List<MessageDTO> messageDTOList = new ArrayList<>();
        final ConsumerRecords<String, String> read = this.consumerService.read(topic, 0);

        for (ConsumerRecord<String, String> record : read.records(topic)) {
            final MessageDTO messageDTO = new MessageDTO(record.value(), record.key());
            messageDTOList.add(messageDTO);
        }
        return ResponseEntity.ok(messageDTOList);
    }
}
