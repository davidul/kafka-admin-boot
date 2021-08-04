package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ConsumerGroupListingDTO(
        @JsonProperty("groupId") String groupId,
        @JsonProperty("isSimple") Boolean isSimple
) {
}
