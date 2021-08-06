package davidul.online.kafkaadminboot.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

@Service
public class ConsumerService {

    public static Properties consumerProperties(String bootstrap, String group){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }

    public ConsumerRecords<String, String> read(String topic, Integer partition){
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties("localhost:9092", "group-1"));
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Set<TopicPartition> topicPartitions = new HashSet<>();
        topicPartitions.add(topicPartition);
        consumer.assign(topicPartitions);

        final Set<TopicPartition> assignment = consumer.assignment();
        consumer.seek(topicPartition, 0);

        final long position = consumer.position(topicPartition);
        final ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
        consumer.close();
        return poll;
    }
}
