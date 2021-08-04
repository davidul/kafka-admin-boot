package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

public record MemberDescriptionDTO(
        @JsonProperty("memberId") String memberId,
        @JsonProperty("groupInstanceId") String groupInstanceId,
        @JsonProperty("clientId") String clientId,
        @JsonProperty("host") String host,
        @JsonProperty("assignment") Set<TopicPartitionDTO> assignment
) {
}
