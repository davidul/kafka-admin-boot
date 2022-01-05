package davidul.online.kafkaadminboot;

import davidul.online.kafkaadminboot.service.ConnectionService;
import davidul.online.kafkaadminboot.service.TopicService;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class KafkaTest {

    @Test
    public void connect() {
        final ConnectionService connectionService = new ConnectionService();
        final AdminClient adminClient = connectionService.adminClient();
        assertThat(adminClient).isNotNull();
    }

    @Test
    public void topicService() {
        final ConnectionService connectionService = new ConnectionService();
        final TopicService topicService = new TopicService(connectionService);
        topicService.createTopic("test-topic");

        final Set<String> strings = topicService.listTopics(false);
        assertThat(strings).contains("test-topic");
    }
}
