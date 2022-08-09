package davidul.online.kafkaadminboot.model.internal;

import org.apache.kafka.common.KafkaFuture;

import java.time.LocalDateTime;

public class KafkaRequest<T> {
    private final LocalDateTime createdAt;

    private final KafkaFuture<T> kafkaFuture;

    private final String createdBy;

    public KafkaRequest(LocalDateTime createdAt, KafkaFuture<T> kafkaFuture, String createdBy) {
        this.createdAt = createdAt;
        this.kafkaFuture = kafkaFuture;
        this.createdBy = createdBy;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public KafkaFuture<T> getKafkaFuture() {
        return kafkaFuture;
    }

    public Class<? extends KafkaFuture> getType(){
        return kafkaFuture.getClass();
    }

    public boolean isDone(){
        return this.kafkaFuture.isDone();
    }

    public boolean isCancelled(){
        return this.kafkaFuture.isCancelled();
    }

    public boolean isCompletedExceptionally(){
        return this.kafkaFuture.isCompletedExceptionally();
    }

    public String getCreatedBy() {
        return createdBy;
    }
}
