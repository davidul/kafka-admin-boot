package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record ClusterDTO(
        @JsonProperty("clusterId") String clusterId,
        @JsonProperty("nodes") List<NodeDTO> nodes,
        @JsonProperty("controller") NodeDTO controller
) {
}
