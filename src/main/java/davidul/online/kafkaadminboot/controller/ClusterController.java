package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.model.ClusterDTO;
import davidul.online.kafkaadminboot.service.ClusterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClusterController {

    private final ClusterService topicService;

    public ClusterController(ClusterService topicService) {
        this.topicService = topicService;
    }

    @GetMapping(value = "/cluster")
    public ResponseEntity<ClusterDTO> getCluster(){
        final ClusterDTO clusterDTO = this.topicService.describeCluster();
        return ResponseEntity.ok(clusterDTO);
    }
}
