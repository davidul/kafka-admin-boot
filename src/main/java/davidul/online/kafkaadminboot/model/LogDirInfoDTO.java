package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public record LogDirInfoDTO(
        @JsonProperty("error") String error,
        @JsonProperty("map") Map<TopicPartitionDTO, ReplicaInfoDTO> replicaInfos
) {
}
