package davidul.online.kafkaadminboot;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import davidul.online.kafkaadminboot.service.ConnectionService;
import davidul.online.kafkaadminboot.service.KafkaResultQueue;
import davidul.online.kafkaadminboot.service.TopicService;
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

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {"admin.timeout=1000"})
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
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
        String topic = topicService.createTopic("test-topic");

        final Set<String> strings = topicService
                .listTopics(false)
                .getTopicNames();
        assertThat(strings).contains("test-topic");
    }

    @Test
    public void list_topics_empty() throws InternalException, KafkaTimeoutException {
        Set<String> strings = topicService.listTopics(false).getTopicNames();
        assertThat(strings.isEmpty()).isTrue();
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
        topicService.createTopic("describe-topic");
        TopicDescription topicDescription = null;
        try {
            topicDescription = topicService.describeTopic("describe-topic");
        }catch(KafkaTimeoutException e){
            String key = e.getKey();
            KafkaRequest<?> kafkaRequest = kafkaResultQueue.get(key);
            topicDescription = (TopicDescription) kafkaRequest.getKafkaFuture().get();
        }
        assertThat(topicDescription).isNotNull();
        assertThat(topicDescription.name()).isEqualTo("describe-topic");
    }

    @Test
    public void describe_topic_not_exist() throws KafkaTimeoutException, InternalException {
        TopicDescription topicDescription = topicService.describeTopic("not-exists");
        assertThat(topicDescription).isNull();
    }

    @Test
    public void describe_topics_all() throws KafkaTimeoutException, InternalException {
        topicService.createTopic("describe-all");
        Map<String, TopicDescription> stringTopicDescriptionMap = topicService.describeTopicsAll(false);
        assertThat(stringTopicDescriptionMap).isNotNull();
    }

    @Test
    public void delete_test() throws KafkaTimeoutException, InternalException, ExecutionException, InterruptedException {
        topicService.createTopic("delete-topic");
        TopicDescription topicDescription = null;
        try {
            topicDescription = topicService.describeTopic("delete-topic");
        }catch (KafkaTimeoutException e){
            String key = e.getKey();
            //KafkaFuture<TopicDescription>
            KafkaRequest<?> kafkaRequest = kafkaResultQueue.get(key);
            KafkaFuture<?> kafkaFuture = kafkaRequest.getKafkaFuture();
            topicDescription = (TopicDescription) kafkaFuture.get();
        }
        assertThat(topicDescription).isNotNull();
        assertThat(topicDescription.name()).isEqualTo("delete-topic");

        topicService.deleteTopic("delete-topic");

        TopicDescription topicDescription1 = null;
        try {
            topicDescription1 = topicService.describeTopic("delete-topic");
        }catch(KafkaTimeoutException e){
            String key = e.getKey();
            try {
                topicDescription1 = (TopicDescription) kafkaResultQueue.get(key).getKafkaFuture().get();
            }catch (UnknownTopicOrPartitionException e1){

            }
        }
        assertThat(topicDescription1).isNull();
    }
}
