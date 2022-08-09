package davidul.online.kafkaadminboot;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.service.ConnectionService;
import davidul.online.kafkaadminboot.service.KafkaResultQueue;
import davidul.online.kafkaadminboot.service.TopicService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TopicServiceTest {

    @Test
    public void connect() {
        final ConnectionService connectionService = new ConnectionService();
        final AdminClient adminClient = connectionService.adminClient();
        assertThat(adminClient).isNotNull();
    }

    @Test
    public void list_topics() throws InternalException {
        final ConnectionService connectionService = new ConnectionService();
        final KafkaResultQueue kafkaResultQueue = new KafkaResultQueue();
        final TopicService topicService = new TopicService(connectionService, kafkaResultQueue);
        topicService.createTopic("test-topic");

        final Set<String> strings = topicService.listTopics(false).getTopicNames();
        assertThat(strings).contains("test-topic");
    }

    @Test
    public void list_topics_empty() throws InternalException {
        final ConnectionService connectionService = new ConnectionService();
        final KafkaResultQueue kafkaResultQueue = new KafkaResultQueue();
        final TopicService topicService = new TopicService(connectionService, kafkaResultQueue);

        Set<String> strings = topicService.listTopics(false).getTopicNames();
        assertThat(strings.isEmpty()).isTrue();
    }

    @Test
    public void list_internal_topics() throws InternalException {
        final ConnectionService connectionService = new ConnectionService();
        final KafkaResultQueue kafkaResultQueue = new KafkaResultQueue();
        final TopicService topicService = new TopicService(connectionService, kafkaResultQueue);

        Set<String> strings = topicService.listTopics(true).getTopicNames();
        assertThat(strings.isEmpty()).isFalse();
    }

    @Test
    public void create_topic() throws InternalException {
        final ConnectionService connectionService = new ConnectionService();
        final KafkaResultQueue kafkaResultQueue = new KafkaResultQueue();
        final TopicService topicService = new TopicService(connectionService, kafkaResultQueue);

        topicService.createTopic("test-topic");
        Set<String> strings = topicService.listTopics(false).getTopicNames();
        assertThat(strings.contains("test-topic")).isTrue();
    }

    @Test
    public void describe_topic(){
        final ConnectionService connectionService = new ConnectionService();
        final KafkaResultQueue kafkaResultQueue = new KafkaResultQueue();
        final TopicService topicService = new TopicService(connectionService, kafkaResultQueue);

        topicService.createTopic("describe-topic");
        TopicDescription topicDescription = topicService.describeTopic("describe-topic");
        assertThat(topicDescription).isNotNull();
        assertThat(topicDescription.name()).isEqualTo("describe-topic");
    }

    @Test
    public void describe_topic_not_exist(){
        final ConnectionService connectionService = new ConnectionService();
        final KafkaResultQueue kafkaResultQueue = new KafkaResultQueue();
        final TopicService topicService = new TopicService(connectionService, kafkaResultQueue);

        TopicDescription topicDescription = topicService.describeTopic("not-exists");
        assertThat(topicDescription).isNull();
    }

    @Test
    public void describe_topics_all(){
        final ConnectionService connectionService = new ConnectionService();
        final KafkaResultQueue kafkaResultQueue = new KafkaResultQueue();
        final TopicService topicService = new TopicService(connectionService, kafkaResultQueue);

        topicService.createTopic("describe-all");
        Map<String, TopicDescription> stringTopicDescriptionMap = topicService.describeTopicsAll(false);
        assertThat(stringTopicDescriptionMap).isNotNull();
    }

    @Test
    public void delete_test(){
        final ConnectionService connectionService = new ConnectionService();
        final KafkaResultQueue kafkaResultQueue = new KafkaResultQueue();
        final TopicService topicService = new TopicService(connectionService, kafkaResultQueue);

        topicService.createTopic("delete-topic");
        TopicDescription topicDescription = topicService.describeTopic("delete-topic");
        assertThat(topicDescription).isNotNull();
        assertThat(topicDescription.name()).isEqualTo("delete-topic");

        topicService.deleteTopic("delete-topic");

        TopicDescription topicDescription1 = topicService.describeTopic("delete-topic");
        assertThat(topicDescription1).isNull();
    }
}
