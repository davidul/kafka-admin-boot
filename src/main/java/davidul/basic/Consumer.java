package davidul.basic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class Consumer {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    public Consumer(String bootstrap){
        this.kafkaConsumer = new KafkaConsumer<>(Config.consumerProperties(bootstrap));
    }

    public void consume(String topic){
        this.kafkaConsumer.subscribe(Collections.singletonList(topic));
        while(true){
            logger.info("Before !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            final ConsumerRecords<String, String> records = this.kafkaConsumer.poll(Duration.ofMillis(1000));
            logger.info("After ##########################################");
            for(ConsumerRecord<String, String> r : records){
                logger.info("Partition: " + r.partition());
                logger.info("Headers: " + r.headers());
                logger.info("Key " + r.key() + " Value:" + r.value());
            }
        }
    }
}
