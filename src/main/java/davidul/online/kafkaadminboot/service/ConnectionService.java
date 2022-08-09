package davidul.online.kafkaadminboot.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ConnectionService {

    private Properties properties;
    private AdminClient adminClient;

    public ConnectionService() {
        String kafka_bootstrap = System.getenv().get("KAFKA_BOOTSTRAP");
        if(kafka_bootstrap == null){
            kafka_bootstrap = "127.0.0.1:9092";
        }
        this.properties = new Properties();
        this.properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_bootstrap);
    }

    public AdminClient adminClient() {
        if (adminClient == null)
            this.adminClient = AdminClient.create(this.properties);
        return adminClient;
    }
}
