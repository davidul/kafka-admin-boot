package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaFutureHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaFutureHandler.class);

    public static <T> T handleFuture(KafkaFuture<T> kafkaFuture, String createdBy, KafkaResultQueue kafkaResultQueue) throws InternalException, KafkaTimeoutException {
        try {
            return kafkaFuture.get(Integer.parseInt("1000"), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception: ", e);
            throw new InternalException(e);
        } catch (TimeoutException e) {
            logger.debug("Timeout exception");
            String key = kafkaResultQueue.add(
                    new KafkaRequest<>(LocalDateTime.now(), kafkaFuture, createdBy));

            throw new KafkaTimeoutException(key);
        }
    }
}
