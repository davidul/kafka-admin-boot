package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ReplicaInfoDTO(
        @JsonProperty("size") Long size,
        @JsonProperty("offsetLag") Long offsetLag,
        @JsonProperty("isFuture") Boolean isFuture
) {
}
