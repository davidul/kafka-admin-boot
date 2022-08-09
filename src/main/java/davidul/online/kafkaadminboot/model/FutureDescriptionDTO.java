package davidul.online.kafkaadminboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public record FutureDescriptionDTO(@JsonProperty String isDone,
                                   @JsonProperty String isCancelled,
                                   @JsonProperty String isCompletedExceptionally,
                                   @JsonProperty LocalDateTime createdAt,
                                   @JsonProperty String type) {
}
