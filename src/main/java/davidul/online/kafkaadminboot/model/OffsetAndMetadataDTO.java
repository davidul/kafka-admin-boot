package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record OffsetAndMetadataDTO(
        @JsonProperty("offset") Long offset,
        @JsonProperty("metadata") String metadata
) {
}
