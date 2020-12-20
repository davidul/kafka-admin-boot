package davidul.cluster;

import davidul.basic.Config;
import davidul.basic.Consumer;
import io.vertx.core.AbstractVerticle;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class SampleConsumer extends AbstractVerticle {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final String topic = "my-topic";

    public SampleConsumer(String bootstrap, String consumerGroup) {
        this.kafkaConsumer = new KafkaConsumer<>(Config.consumerProperties(bootstrap, consumerGroup));
    }

    @Override
    public void start() {

        final ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                SampleConsumer.this.logger.info("Partition revoked");
                Set<TopicPartition> tp = new HashSet<>(partitions);
                final Map<TopicPartition, OffsetAndMetadata> committed = kafkaConsumer.committed(tp);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                SampleConsumer.this.logger.info("Partition Assigned");
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                SampleConsumer.this.logger.info("Partition Lost");
            }
        };

        consume(topic, consumerRebalanceListener);
    }

    public void consume(String topic, ConsumerRebalanceListener consumerRebalanceListener) {
        this.kafkaConsumer.subscribe(Collections.singletonList(topic), consumerRebalanceListener);
        while (true) {
            final ConsumerRecords<String, String> records = this.kafkaConsumer.poll(Duration.ofMillis(1000));

            this.kafkaConsumer.pause(this.kafkaConsumer.assignment());

            Flux
                    .fromIterable(records)
                    .timeout(Duration.ofMillis(800))
                    .doOnError(TimeoutException.class, e -> kafkaConsumer.poll(Duration.ZERO))
                    .subscribe(r -> {
                                logger.info("Member Id: " + this.kafkaConsumer.groupMetadata().memberId() + " GroupId: " + this.kafkaConsumer.groupMetadata().groupId());
                                logger.info("Partition: " + r.partition());
                                logger.info("Headers: " + r.headers());
                                logger.info("Key " + r.key() + " Value:" + r.value());
                            }, Throwable::printStackTrace
                            , () -> this.kafkaConsumer.resume(this.kafkaConsumer.assignment()));

        }
    }


}


