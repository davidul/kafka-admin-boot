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
        this.properties = new Properties();
        this.properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    public AdminClient adminClient() {
        if (adminClient == null)
            this.adminClient = AdminClient.create(this.properties);
        return adminClient;
    }
}
