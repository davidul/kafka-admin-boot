package davidul.online.kafkaadminboot.controller;

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
    public ResponseEntity<Map<Integer, Map<String, LogDirInfoDTO>>> getLogDirs(){
        final Map<Integer, Map<String, LogDirInfoDTO>> integerMapMap = this.topicService.describeLogDirs(Collections.singleton(0));
        return ResponseEntity.ok(integerMapMap);
    }
}
