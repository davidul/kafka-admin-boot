package davidul.online.kafkaadminboot.model.internal;

import java.util.Set;

public class ListTopicsDTO {
    private final Set<String> topicNames;
    private final Boolean isTimeout;
    private final String futureKey;

    public ListTopicsDTO(Set<String> topicNames, Boolean isTimeout, String futureKey) {
        this.topicNames = topicNames;
        this.isTimeout = isTimeout;
        this.futureKey = futureKey;
    }

    public Set<String> getTopicNames() {
        return topicNames;
    }

    public Boolean getTimeout() {
        return isTimeout;
    }

    public String getFutureKey() {
        return futureKey;
    }
}
