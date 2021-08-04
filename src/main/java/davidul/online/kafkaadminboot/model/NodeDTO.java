package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record NodeDTO(
        @JsonProperty("id") Integer id,
        @JsonProperty("host") String host,
        @JsonProperty("port") Integer port,
        @JsonProperty("rack") String rack
) {
}
