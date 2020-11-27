package davidul.basic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testcontainers.containers.KafkaContainer;

import java.util.Properties;

public class KafkaContainerSetup {

    public static AdminClient setup(KafkaContainer kafka){
        final String bootstrapServers = kafka.getBootstrapServers();
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(properties);
    }
}
