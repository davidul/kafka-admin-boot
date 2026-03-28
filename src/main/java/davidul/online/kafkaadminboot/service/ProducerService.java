package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.model.RecordMetadataDTO;
import davidul.online.kafkaadminboot.model.TopicPartitionDTO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Service
public class ProducerService {

    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);

    private final String bootstrapServers;

    /**
     * Bootstrap address resolved from the {@code KAFKA_BOOTSTRAP} environment variable
     * (via Spring's {@code Environment} abstraction), consistent with {@link ConnectionService}.
     */
    public ProducerService(
            @Value("${KAFKA_BOOTSTRAP:localhost:9092}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * Builds producer {@link Properties} from the given bootstrap address.
     *
     * <p>Previously this method silently ignored its {@code bootstrap} parameter
     * and read {@code System.getenv("KAFKA_BOOTSTRAP")} directly (C4 bug — now fixed).
     */
    public static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }

    /**
     * Sends {@code message} to {@code topic} and blocks until the broker acknowledges
     * the write (or an error occurs).
     *
     * <p>The {@link KafkaProducer} is created and closed on every call (see M1 in the
     * architecture assessment for the known trade-off). Try-with-resources guarantees
     * the producer is always closed and its internal threads released.
     *
     * @return metadata describing the committed record (offset, partition, timestamps, sizes)
     * @throws InternalException if the broker rejects the record ({@link ExecutionException})
     *                           or the calling thread is interrupted while waiting for the ack
     */
    public RecordMetadataDTO produce(String topic, String message) throws InternalException {
        logger.debug("Producing message to topic={}", topic);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties(bootstrapServers))) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            RecordMetadata metadata = producer.send(record).get();   // blocks until broker ack
            RecordMetadataDTO result = new RecordMetadataDTO(
                    metadata.offset(),
                    metadata.timestamp(),
                    metadata.serializedKeySize(),
                    metadata.serializedValueSize(),
                    new TopicPartitionDTO(metadata.topic(), metadata.partition()));
            logger.debug("Message committed: topic={}, partition={}, offset={}",
                    metadata.topic(), metadata.partition(), metadata.offset());
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();   // restore interrupt flag
            throw new InternalException(e);
        } catch (ExecutionException e) {
            logger.error("Failed to produce message to topic={}: {}", topic, e.getCause().getMessage(), e);
            throw new InternalException(e);
        }
    }
}
