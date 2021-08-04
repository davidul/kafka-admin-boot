package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record TopicPartitionsDTO(@JsonProperty("name") String name,
                                 @JsonProperty("partitions") List<PartitionInfoDTO> partitions) {
}
