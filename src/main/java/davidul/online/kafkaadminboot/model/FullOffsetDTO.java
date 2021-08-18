package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record FullOffsetDTO(@JsonProperty("start") Long start, @JsonProperty("end") Long end) {
}
