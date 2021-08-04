package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record MessageDTO(@JsonProperty("key") String key,
                         @JsonProperty("value") String value) {
}
