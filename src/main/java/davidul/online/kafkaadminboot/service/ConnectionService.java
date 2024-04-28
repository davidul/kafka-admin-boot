package davidul.online.kafkaadminboot.service;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ConnectionService {

    private Properties properties;
    private AdminClient adminClient;


    public ConnectionService() {
        String kafka_bootstrap = System.getenv().get("KAFKA_BOOTSTRAP");
        if(kafka_bootstrap == null){
            kafka_bootstrap = "localhost:9092";
        }
        this.properties = new Properties();
        this.properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_bootstrap);
//        this.properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
//        this.properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
//        this.properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
//        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule requires username=\"%s\" password=\"%s\"";
//        String jaasCfg = String.format(jaasTemplate, "admin","password");
//        this.properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasCfg);
//        this.properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

    }

//    @PostConstruct
//    public void init(){
//        this.adminClient = AdminClient.create(this.properties);
//    }

    public AdminClient adminClient() {
        if (adminClient == null){
            this.adminClient = AdminClient.create(this.properties);
        }
        return adminClient;
    }

    public AdminClient adminClient(Properties properties){
        if (adminClient == null){
            if (properties == null){
                this.adminClient = adminClient();
            }
            this.adminClient = AdminClient.create(properties);
        }
        return adminClient;
    }

}
