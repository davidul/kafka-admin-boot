package davidul.online.kafkaadminboot;

import davidul.online.kafkaadminboot.service.ConnectionService;
import davidul.online.kafkaadminboot.service.TopicService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

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
    public void list_topics() {
        final ConnectionService connectionService = new ConnectionService();
        final TopicService topicService = new TopicService(connectionService);
        topicService.createTopic("test-topic");

        final Set<String> strings = topicService.listTopics(false);
        assertThat(strings).contains("test-topic");
    }

    @Test
    public void list_topics_empty(){
        final ConnectionService connectionService = new ConnectionService();
        final TopicService topicService = new TopicService(connectionService);

        Set<String> strings = topicService.listTopics(false);
        assertThat(strings.isEmpty()).isTrue();
    }

    @Test
    public void list_internal_topics(){
        final ConnectionService connectionService = new ConnectionService();
        final TopicService topicService = new TopicService(connectionService);

        Set<String> strings = topicService.listTopics(true);
        assertThat(strings.isEmpty()).isFalse();
    }

    @Test
    public void create_topic(){
        final ConnectionService connectionService = new ConnectionService();
        final TopicService topicService = new TopicService(connectionService);

        topicService.createTopic("test-topic");
        Set<String> strings = topicService.listTopics(false);
        assertThat(strings.contains("test-topic")).isTrue();
    }

    @Test
    public void describe_topic(){
        final ConnectionService connectionService = new ConnectionService();
        final TopicService topicService = new TopicService(connectionService);

        topicService.createTopic("describe-topic");
        TopicDescription topicDescription = topicService.describeTopic("describe-topic");
        assertThat(topicDescription).isNotNull();
        assertThat(topicDescription.name()).isEqualTo("describe-topic");
    }

    @Test
    public void describe_topic_not_exist(){
        final ConnectionService connectionService = new ConnectionService();
        final TopicService topicService = new TopicService(connectionService);

        TopicDescription topicDescription = topicService.describeTopic("not-exists");
        assertThat(topicDescription).isNull();
    }

    @Test
    public void describe_topics_all(){
        final ConnectionService connectionService = new ConnectionService();
        final TopicService topicService = new TopicService(connectionService);

        topicService.createTopic("describe-all");
        Map<String, TopicDescription> stringTopicDescriptionMap = topicService.describeTopicsAll(false);
        assertThat(stringTopicDescriptionMap).isNotNull();
    }

    @Test
    public void delete_test(){
        final ConnectionService connectionService = new ConnectionService();
        final TopicService topicService = new TopicService(connectionService);

        topicService.createTopic("delete-topic");
        TopicDescription topicDescription = topicService.describeTopic("delete-topic");
        assertThat(topicDescription).isNotNull();
        assertThat(topicDescription.name()).isEqualTo("delete-topic");

        topicService.deleteTopic("delete-topic");

        TopicDescription topicDescription1 = topicService.describeTopic("delete-topic");
        assertThat(topicDescription1).isNull();
    }
}
