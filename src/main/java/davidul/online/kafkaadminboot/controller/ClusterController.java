package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.model.ClusterDTO;
import davidul.online.kafkaadminboot.service.ClusterService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClusterController {

    private final ClusterService clusterService;

    public ClusterController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Operation(summary = "Describe the Kafka cluster")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Cluster information returned",
                    content = { @Content(mediaType = "application/json",
                            schema = @Schema(implementation = ClusterDTO.class)) }),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content)
    })
    @GetMapping(value = "/cluster", produces = "application/json")
    public ResponseEntity<ClusterDTO> getCluster() {
        try {
            final ClusterDTO clusterDTO = this.clusterService.describeCluster();
            return ResponseEntity.ok(clusterDTO);
        } catch (InternalException e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
