package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TopicPartitionDTO(@JsonProperty("name") String name,
                                @JsonProperty("partition") Integer partition) {
}
