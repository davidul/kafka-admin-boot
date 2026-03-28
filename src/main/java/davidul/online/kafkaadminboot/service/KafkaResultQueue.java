package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory registry that holds {@link KafkaRequest} entries for async futures
 * that timed out before a result could be returned to the caller.
 *
 * <h3>Eviction</h3>
 * A scheduled background task removes entries that are older than
 * {@code queue.eviction.ttl-minutes} (default: 30 min), preventing unbounded
 * memory growth when clients never come back to collect their results.
 * The eviction task runs every {@code queue.eviction.interval-ms} (default: 60 s).
 */
@Service
public class KafkaResultQueue {

    private static final Logger logger = LoggerFactory.getLogger(KafkaResultQueue.class);

    private final Map<String, KafkaRequest<?>> mp = new ConcurrentHashMap<>();
    private final long ttlMinutes;

    public KafkaResultQueue(
            @Value("${queue.eviction.ttl-minutes:30}") long ttlMinutes) {
        this.ttlMinutes = ttlMinutes;
        logger.info("KafkaResultQueue initialised: TTL={}min", ttlMinutes);
    }

    // -------------------------------------------------------------------------
    // Queue operations
    // -------------------------------------------------------------------------

    /**
     * Stores a {@link KafkaRequest} and returns the generated UUID key.
     */
    public String add(KafkaRequest<?> kafkaRequest) {
        String uuid = UUID.randomUUID().toString();
        mp.put(uuid, kafkaRequest);
        logger.debug("Enqueued future: key={}, createdBy={}", uuid, kafkaRequest.getCreatedBy());
        return uuid;
    }

    /**
     * Retrieves a {@link KafkaRequest} without removing it.
     * Returns {@code null} if the key is unknown or has already been evicted.
     */
    public KafkaRequest<?> get(String uuid) {
        return mp.get(uuid);
    }

    /**
     * Removes and returns a {@link KafkaRequest}, or {@code null} if absent.
     */
    public KafkaRequest<?> remove(String key) {
        return mp.remove(key);
    }

    /** Returns the set of all live queue keys. */
    public Set<String> keys() {
        return mp.keySet();
    }

    /** Returns the number of entries currently in the queue. */
    public int size() {
        return mp.size();
    }

    // -------------------------------------------------------------------------
    // Eviction
    // -------------------------------------------------------------------------

    /**
     * Removes entries whose {@link KafkaRequest#getCreatedAt() createdAt} timestamp
     * is older than {@code queue.eviction.ttl-minutes}.
     *
     * <p>Runs on a fixed delay of {@code queue.eviction.interval-ms} after the
     * previous run completes (not on a fixed rate), so a slow eviction pass cannot
     * overlap with the next one.
     *
     * <p>Package-private to allow direct invocation from unit tests.
     */
    @Scheduled(fixedDelayString = "${queue.eviction.interval-ms:60000}")
    void evictExpired() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(ttlMinutes);
        AtomicInteger count = new AtomicInteger();

        mp.entrySet().removeIf(entry -> {
            boolean expired = entry.getValue().getCreatedAt().isBefore(cutoff);
            if (expired) {
                count.incrementAndGet();
                logger.debug("Evicting expired queue entry: key={}, createdBy={}, createdAt={}",
                        entry.getKey(),
                        entry.getValue().getCreatedBy(),
                        entry.getValue().getCreatedAt());
            }
            return expired;
        });

        if (count.get() > 0) {
            logger.info("KafkaResultQueue eviction: removed {} expired entr{} (TTL={}min, remaining={})",
                    count.get(), count.get() == 1 ? "y" : "ies", ttlMinutes, mp.size());
        } else {
            logger.debug("KafkaResultQueue eviction pass completed: no expired entries (size={})", mp.size());
        }
    }
}
