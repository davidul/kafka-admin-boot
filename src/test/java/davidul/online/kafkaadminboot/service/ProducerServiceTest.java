package davidul.online.kafkaadminboot.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class ProducerServiceTest {

    @Test
    void produce() {
        ConnectionService connectionService = new ConnectionService();
        ProducerService producerService = new ProducerService();
        TopicService topicService = new TopicService(connectionService);
        topicService.createTopic("test-topic");
        producerService.produce("test-topic", "message-1");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(ConsumerService.consumerProperties("localhost:9092", "group-1"));
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);

       // kafkaConsumer.subscribe(Collections.singletonList("test-topic"));
        kafkaConsumer.assign(Collections.singleton(topicPartition));

        assertThat(kafkaConsumer.position(topicPartition)).isEqualTo(0);

        Iterable<ConsumerRecord<String, String>> records = kafkaConsumer.poll(Duration.ofMillis(100)).records("test-topic");
        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        assertThat(iterator.hasNext()).isTrue();

        records.forEach(record -> {
            assertThat(record.value()).isEqualTo("message-1");
        });

        assertThat(kafkaConsumer.position(topicPartition)).isEqualTo(1);
    }
}