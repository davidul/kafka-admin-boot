package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record OffsetDTO(@JsonProperty("position") String position,
                        @JsonProperty("offset") Long offset) {
}
