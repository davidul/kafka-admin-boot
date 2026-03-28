package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link KafkaResultQueue} eviction and basic queue semantics.
 * No Spring context or Kafka broker required.
 */
class KafkaResultQueueTest {

    // TTL of 1 minute — entries created >1 min ago are expired.
    private KafkaResultQueue queue;

    @BeforeEach
    void setUp() {
        queue = new KafkaResultQueue(1 /* ttlMinutes */);
    }

    // -------------------------------------------------------------------------
    // Basic operations
    // -------------------------------------------------------------------------

    @Test
    void add_returnsUniqueKeys() {
        String key1 = queue.add(freshRequest());
        String key2 = queue.add(freshRequest());

        assertThat(key1).isNotEqualTo(key2);
        assertThat(queue.size()).isEqualTo(2);
    }

    @Test
    void get_returnsStoredRequest() {
        KafkaRequest<?> request = freshRequest();
        String key = queue.add(request);

        assertThat(queue.get(key)).isSameAs(request);
    }

    @Test
    void get_returnsNull_forUnknownKey() {
        assertThat(queue.get("nonexistent-uuid")).isNull();
    }

    @Test
    void remove_deletesEntry_andReturnsIt() {
        String key = queue.add(freshRequest());

        KafkaRequest<?> removed = queue.remove(key);

        assertThat(removed).isNotNull();
        assertThat(queue.get(key)).isNull();
        assertThat(queue.size()).isZero();
    }

    @Test
    void keys_reflectsCurrentEntries() {
        String k1 = queue.add(freshRequest());
        String k2 = queue.add(freshRequest());

        assertThat(queue.keys()).containsExactlyInAnyOrder(k1, k2);
    }

    // -------------------------------------------------------------------------
    // Eviction
    // -------------------------------------------------------------------------

    @Test
    void evictExpired_removesExpiredEntries() {
        // entry created 2 minutes ago — beyond the 1-minute TTL
        String expiredKey = queue.add(expiredRequest(2));
        assertThat(queue.size()).isEqualTo(1);

        queue.evictExpired();

        assertThat(queue.get(expiredKey)).isNull();
        assertThat(queue.size()).isZero();
    }

    @Test
    void evictExpired_retainsFreshEntries() {
        String freshKey = queue.add(freshRequest());
        assertThat(queue.size()).isEqualTo(1);

        queue.evictExpired();

        assertThat(queue.get(freshKey)).isNotNull();
        assertThat(queue.size()).isEqualTo(1);
    }

    @Test
    void evictExpired_onlyRemovesExpired_whenMixed() {
        String expiredKey = queue.add(expiredRequest(5));
        String freshKey   = queue.add(freshRequest());

        queue.evictExpired();

        assertThat(queue.get(expiredKey)).isNull();
        assertThat(queue.get(freshKey)).isNotNull();
        assertThat(queue.size()).isEqualTo(1);
    }

    @Test
    void evictExpired_isIdempotent_onEmptyQueue() {
        // should not throw
        queue.evictExpired();
        queue.evictExpired();
        assertThat(queue.size()).isZero();
    }

    @Test
    void evictExpired_removesMultipleExpiredEntries() {
        for (int i = 0; i < 5; i++) {
            queue.add(expiredRequest(i + 2));
        }
        queue.add(freshRequest()); // one fresh entry

        queue.evictExpired();

        assertThat(queue.size()).isEqualTo(1);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** A request timestamped right now — within any reasonable TTL. */
    private static KafkaRequest<?> freshRequest() {
        return new KafkaRequest<>(LocalDateTime.now(), KafkaFuture.completedFuture(null), "test");
    }

    /** A request whose {@code createdAt} is {@code minutesAgo} minutes in the past. */
    private static KafkaRequest<?> expiredRequest(long minutesAgo) {
        return new KafkaRequest<>(
                LocalDateTime.now().minusMinutes(minutesAgo),
                KafkaFuture.completedFuture(null),
                "test-expired");
    }
}

