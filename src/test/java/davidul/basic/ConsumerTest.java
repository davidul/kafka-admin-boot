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

public class ConsumerTest {

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    @Test
    public void _1(){
        final AdminClient adminClient = KafkaContainerSetup.setup(kafka);
        final NewTopic first_topic = new NewTopic("first_topic", 1, (short) 1);
        adminClient.createTopics(Collections.singletonList(first_topic));

        final Gson gson = new Gson();
        final Producer producer = new Producer(kafka.getBootstrapServers());
        final ReactorProducer reactorProducer = new ReactorProducer();
        final List<ReactorProducer.Event> events = reactorProducer.getEvents(1000);
        final var map = events.map(f -> new ProducerRecord<>(first_topic.name(), f.getKey(), gson.toJson(f)));
        producer.produce(map);
        /*reactorProducer.produceEvents(keyValue -> {
            producer.produce(first_topic.name(), keyValue.getT1().toString(), keyValue.getT2());
        });*/

        final Consumer consumer = new Consumer(kafka.getBootstrapServers());
        consumer.consume(first_topic.name());
    }
}
