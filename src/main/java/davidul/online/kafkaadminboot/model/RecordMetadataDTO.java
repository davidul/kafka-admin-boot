package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RecordMetadataDTO(
        @JsonProperty("offset") Long offset,
        @JsonProperty("timestamp") Long timestamp,
        @JsonProperty("serializedKeySize") Integer serializedKeySize,
        @JsonProperty("serializedValueSize") Integer serializedValueSize,
        @JsonProperty("topicPartitionDTO") TopicPartitionDTO topicPartitionDTO
) {
}
