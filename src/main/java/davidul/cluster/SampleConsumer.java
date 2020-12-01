package davidul.cluster;

import davidul.basic.Config;
import davidul.basic.Consumer;
import io.vertx.core.AbstractVerticle;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class SampleConsumer extends AbstractVerticle {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final String topic = "my-topic";

    public SampleConsumer(String bootstrap, String consumerGroup) {
        this.kafkaConsumer =  new KafkaConsumer<>(Config.consumerProperties(bootstrap, consumerGroup));
    }

    @Override
    public void start() throws Exception {
        consume(topic);
    }

    public void consume(String topic){
        this.kafkaConsumer.subscribe(Collections.singletonList(topic));
        while(true){
            final ConsumerRecords<String, String> records = this.kafkaConsumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> r : records){
                logger.info("Member Id: " + this.kafkaConsumer.groupMetadata().memberId() + " GroupId: " + this.kafkaConsumer.groupMetadata().groupId());
                logger.info("Partition: " + r.partition());
                logger.info("Headers: " + r.headers());
                logger.info("Key " + r.key() + " Value:" + r.value());
            }
        }
    }
}
