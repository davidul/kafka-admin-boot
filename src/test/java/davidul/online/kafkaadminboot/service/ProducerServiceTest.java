package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.RecordMetadataDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class ProducerServiceTest {

    @Test
    void produce() throws InternalException, KafkaTimeoutException {
        ConnectionService connectionService =
                new ConnectionService("localhost:9092", "PLAINTEXT", "SCRAM-SHA-512", "", "", "https");
        ProducerService producerService = new ProducerService("localhost:9092");
        final KafkaResultQueue kafkaResultQueue = new KafkaResultQueue(30);
        TopicService topicService = new TopicService(connectionService, kafkaResultQueue, 5000);
        topicService.createTopic("test-topic");

        RecordMetadataDTO metadata = producerService.produce("test-topic", "message-1");

        // Verify returned metadata is populated
        assertThat(metadata).isNotNull();
        assertThat(metadata.offset()).isGreaterThanOrEqualTo(0);
        assertThat(metadata.topicPartitionDTO()).isNotNull();
        assertThat(metadata.topicPartitionDTO().name()).isEqualTo("test-topic");
        assertThat(metadata.topicPartitionDTO().partition()).isEqualTo(0);
        assertThat(metadata.serializedValueSize()).isGreaterThan(0);

        // Verify the message was actually committed to the broker
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(
                ConsumerService.consumerProperties("localhost:9092", "group-1"));
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        kafkaConsumer.assign(Collections.singleton(topicPartition));
        kafkaConsumer.seekToBeginning(Collections.singleton(topicPartition));

        Iterable<ConsumerRecord<String, String>> records =
                kafkaConsumer.poll(Duration.ofMillis(3000)).records("test-topic");
        kafkaConsumer.close();

        assertThat(records).extracting(ConsumerRecord::value).contains("message-1");
    }
}