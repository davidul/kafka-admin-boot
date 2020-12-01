package davidul.cluster;

import io.vavr.collection.List;
import io.vertx.core.AbstractVerticle;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Main extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    @Override
    public void start() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.101.3:9092,172.18.101.4:9092,172.18.101.5:9092");
        final AdminClient adminClient = AdminClient.create(properties);
        final NewTopic newTopic = new NewTopic("my-topic", 3, (short) 3);
        final CreateTopicsResult topics = adminClient.createTopics(List.of(newTopic).asJava());
        topics.values().get("my-topic").get(5, TimeUnit.SECONDS);
        LOGGER.info("Topoics created");
        vertx.deployVerticle(new SampleProducer("172.18.101.3:9092,172.18.101.4:9092,172.18.101.5:9092"));
        vertx.deployVerticle(new SampleConsumer("172.18.101.3:9092,172.18.101.4:9092,172.18.101.5:9092", "group-1"));
        vertx.deployVerticle(new SampleConsumer("172.18.101.3:9092,172.18.101.4:9092,172.18.101.5:9092", "group-1"));
        vertx.deployVerticle(new SampleConsumer("172.18.101.3:9092,172.18.101.4:9092,172.18.101.5:9092", "group-1"));
        vertx.deployVerticle(new SampleConsumer("172.18.101.3:9092,172.18.101.4:9092,172.18.101.5:9092", "group-2"));
        vertx.deployVerticle(new SampleConsumer("172.18.101.3:9092,172.18.101.4:9092,172.18.101.5:9092", "group-3"));
    }
}
