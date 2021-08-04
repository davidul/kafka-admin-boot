package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PartitionInfoDTO(@JsonProperty("partition") Integer partition,
                               @JsonProperty("leader") String leader) {
}
