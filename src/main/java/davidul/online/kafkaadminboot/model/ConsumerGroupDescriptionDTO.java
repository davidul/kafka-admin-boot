package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record ConsumerGroupDescriptionDTO(
        @JsonProperty("groupId") String groupId,
        @JsonProperty("isSimpleConsumerGroup") Boolean isSimpleConsumerGroup,
        @JsonProperty("members") List<MemberDescriptionDTO> members,
        @JsonProperty("partitionAssignor") String partitionAssignor,
        @JsonProperty("state") String state,
        @JsonProperty("coordinator") NodeDTO coordinator
) {
}
