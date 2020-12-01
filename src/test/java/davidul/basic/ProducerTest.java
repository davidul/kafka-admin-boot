package davidul.basic;

import com.google.gson.Gson;
import io.vavr.collection.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;

public class ProducerTest {

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    @Test
    public void _1(){
        final String bootstrapServers = kafka.getBootstrapServers();
        final AdminClient adminClient = KafkaContainerSetup.setup(kafka);
        final NewTopic first_topic = new NewTopic("first_topic", 1, (short) 1);
        adminClient.createTopics(Collections.singletonList(first_topic));


        final Gson gson = new Gson();
        final Producer producer = new Producer(bootstrapServers);
        final ReactorProducer reactorProducer = new ReactorProducer();
        final List<ReactorProducer.Event> events = reactorProducer.getEvents(10);
        final List<ProducerRecord<String, String>> map = events.map(f -> new ProducerRecord<>(first_topic.name(), f.getKey(), gson.toJson(f)));
        producer.produce(map);
    }


}
