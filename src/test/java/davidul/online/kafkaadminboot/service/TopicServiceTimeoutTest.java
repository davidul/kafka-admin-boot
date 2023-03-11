package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {"admin.timeout=1"})
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"},
        topics = {"topic-service-test"})
public class TopicServiceTimeoutTest {

    @Autowired
    private TopicService topicService;

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private KafkaResultQueue kafkaResultQueue;

    @Test
    public void list_topics() throws InternalException {
        Set<String> strings;
        try {
            strings = topicService
                    .listTopics(false)
                    .getTopicNames();
        } catch (KafkaTimeoutException e) {
            String key = e.getKey();
            KafkaRequest<?> kafkaRequest = kafkaResultQueue.get(key);
            KafkaFuture<?> kafkaFuture = kafkaRequest.getKafkaFuture();
            try {
                strings = (Set<String>)kafkaFuture.get();
                assertThat(strings).contains("topic-service-test");
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            } catch (ExecutionException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
