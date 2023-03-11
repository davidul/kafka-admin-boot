package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest(properties = {"admin.timeout=1000"})
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"},
        topics = {"test-topic", "delete-topic"})
public class TopicServiceTest {

    @Autowired
    private TopicService topicService;

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private KafkaResultQueue kafkaResultQueue;

    @Test
    public void connect() {
        final ConnectionService connectionService = new ConnectionService();
        final AdminClient adminClient = connectionService.adminClient();
        assertThat(adminClient).isNotNull();
    }

    @Test
    public void list_topics() throws InternalException, KafkaTimeoutException {
        final Set<String> strings = topicService
                .listTopics(false)
                .getTopicNames();
        assertThat(strings).contains("test-topic");
    }

    @Test
    public void list_topics_empty() throws InternalException, KafkaTimeoutException {
        // Set<String> strings = topicService.listTopics(false).getTopicNames();
        //s assertThat(strings.isEmpty()).isTrue();
    }

    @Test
    public void list_internal_topics() throws InternalException, KafkaTimeoutException {
        Set<String> strings = topicService.listTopics(true).getTopicNames();
        assertThat(strings.isEmpty()).isFalse();
    }

    @Test
    public void create_topic() throws InternalException, KafkaTimeoutException {
        topicService.createTopic("test-topic-1");
        Set<String> strings = topicService.listTopics(false).getTopicNames();
        assertThat(strings.contains("test-topic-1")).isTrue();
    }

    @Test
    public void describe_topic() throws KafkaTimeoutException, InternalException, ExecutionException, InterruptedException {
        TopicDescription topicDescription = null;
        try {
            topicDescription = topicService.describeTopic("test-topic");
        } catch (KafkaTimeoutException e) {
            String key = e.getKey();
            KafkaRequest<?> kafkaRequest = kafkaResultQueue.get(key);
            topicDescription = (TopicDescription) kafkaRequest.getKafkaFuture().get();
        }
        assertThat(topicDescription).isNotNull();
        assertThat(topicDescription.name()).isEqualTo("test-topic");
    }

    @Test
    public void describe_topic_not_exist() {
        assertThatExceptionOfType(InternalException.class)
                .isThrownBy(() ->
                        topicService.describeTopic("not-exists")
                );
    }

    @Test
    public void describe_topics_all() throws KafkaTimeoutException, InternalException {
        topicService.createTopic("describe-all");
        Map<String, TopicDescription> stringTopicDescriptionMap = topicService.describeTopicsAll(false);
        assertThat(stringTopicDescriptionMap).isNotNull();
    }

    @Test
    public void delete_test() throws KafkaTimeoutException, InternalException, ExecutionException, InterruptedException {
        TopicDescription topicDescription;
        try {
            topicDescription = topicService.describeTopic("delete-topic");
        } catch (KafkaTimeoutException e) {
            String key = e.getKey();
            KafkaRequest<?> kafkaRequest = kafkaResultQueue.get(key);
            KafkaFuture<?> kafkaFuture = kafkaRequest.getKafkaFuture();
            topicDescription = (TopicDescription) kafkaFuture.get();
        }
        assertThat(topicDescription).isNotNull();
        assertThat(topicDescription.name()).isEqualTo("delete-topic");

        topicService.deleteTopic("delete-topic");

        assertThatThrownBy(() -> topicService.describeTopic("delete-topic"))
                .hasCauseInstanceOf(ExecutionException.class);
    }
}
