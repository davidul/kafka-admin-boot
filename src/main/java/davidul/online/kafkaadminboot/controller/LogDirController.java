package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.LogDirInfoDTO;
import davidul.online.kafkaadminboot.service.TopicService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;

@RestController
public class LogDirController {

    private final TopicService topicService;

    public LogDirController(TopicService topicService) {
        this.topicService = topicService;
    }

    @GetMapping(value = "/logdirs")
    public ResponseEntity<Map<Integer, Map<String, LogDirInfoDTO>>> getLogDirs() {
        try {
            return ResponseEntity.ok(this.topicService.describeLogDirs(Collections.singleton(0)));
        } catch (KafkaTimeoutException e) {
            return ResponseEntity.accepted().header("queue-id", e.getKey()).build();
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
