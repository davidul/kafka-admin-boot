package davidul.online.kafkaadminboot.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ConnectionService {

    private final Properties properties;

    /**
     * volatile ensures that the double-checked locking pattern in {@link #adminClient()}
     * is safe under the Java Memory Model (JLS 17.4).
     */
    private volatile AdminClient adminClient;

    /**
     * Bootstrap address is resolved by Spring from the {@code KAFKA_BOOTSTRAP} environment
     * variable (via Spring's {@code Environment} abstraction), falling back to
     * {@code localhost:9092} when the variable is absent.
     *
     * <p>To override in tests use:
     * {@code @SpringBootTest(properties = "KAFKA_BOOTSTRAP=localhost:9092")}
     */
    public ConnectionService(
            @Value("${KAFKA_BOOTSTRAP:localhost:9092}") String bootstrapServers) {
        this.properties = new Properties();
        this.properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // SASL/SSL support is exploratory — uncomment and extend when needed:
        // properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        // properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        // properties.put(SaslConfigs.SASL_JAAS_CONFIG, "...");
        // properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    }

    /**
     * Returns the shared {@link AdminClient}, creating it lazily on first call.
     *
     * <p>Thread-safe: uses double-checked locking with a {@code volatile} field so that
     * concurrent callers can never observe a partially-constructed client or create more
     * than one instance.
     */
    public AdminClient adminClient() {
        if (adminClient == null) {
            synchronized (this) {
                if (adminClient == null) {
                    this.adminClient = AdminClient.create(this.properties);
                }
            }
        }
        return adminClient;
    }

    /**
     * Returns a shared {@link AdminClient} built from {@code overrideProperties}.
     * When {@code overrideProperties} is {@code null} the default properties are used.
     *
     * <p>Note: once the client has been created the override is ignored on subsequent
     * calls — the same instance is returned.
     */
    public AdminClient adminClient(Properties overrideProperties) {
        if (overrideProperties == null) {
            return adminClient();
        }
        if (adminClient == null) {
            synchronized (this) {
                if (adminClient == null) {
                    this.adminClient = AdminClient.create(overrideProperties);
                }
            }
        }
        return adminClient;
    }

}
