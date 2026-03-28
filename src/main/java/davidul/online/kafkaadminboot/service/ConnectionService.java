package davidul.online.kafkaadminboot.service;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ConnectionService {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionService.class);

    private final Properties properties;

    /**
     * volatile ensures that the double-checked locking pattern in {@link #adminClient()}
     * is safe under the Java Memory Model (JLS 17.4).
     */
    private volatile AdminClient adminClient;

    /**
     * All connection parameters are resolved by Spring from environment variables
     * (or any source in the {@code Environment} abstraction) with safe defaults.
     *
     * <p>Supported security protocols:
     * <ul>
     *   <li>{@code PLAINTEXT}   – no auth, no encryption (default)</li>
     *   <li>{@code SSL}         – TLS encryption only</li>
     *   <li>{@code SASL_PLAINTEXT} – SASL auth, no encryption</li>
     *   <li>{@code SASL_SSL}    – SASL auth + TLS encryption</li>
     * </ul>
     *
     * <p>Supported SASL mechanisms (when protocol is {@code SASL_*}):
     * {@code PLAIN}, {@code SCRAM-SHA-256}, {@code SCRAM-SHA-512}
     *
     * <p>Example environment for SASL_SSL:
     * <pre>
     *   KAFKA_BOOTSTRAP=broker:9093
     *   KAFKA_SECURITY_PROTOCOL=SASL_SSL
     *   KAFKA_SASL_MECHANISM=SCRAM-SHA-512
     *   KAFKA_SASL_USERNAME=admin
     *   KAFKA_SASL_PASSWORD=secret
     *   KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=https
     * </pre>
     */
    public ConnectionService(
            @Value("${KAFKA_BOOTSTRAP:localhost:9092}") String bootstrapServers,
            @Value("${KAFKA_SECURITY_PROTOCOL:PLAINTEXT}") String securityProtocol,
            @Value("${KAFKA_SASL_MECHANISM:SCRAM-SHA-512}") String saslMechanism,
            @Value("${KAFKA_SASL_USERNAME:}") String saslUsername,
            @Value("${KAFKA_SASL_PASSWORD:}") String saslPassword,
            @Value("${KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM:https}") String sslEndpointIdentAlgorithm) {

        this.properties = new Properties();
        this.properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        boolean isSasl = securityProtocol.startsWith("SASL_");
        boolean isSsl  = securityProtocol.contains("SSL");

        if (isSasl) {
            configureSasl(saslMechanism, saslUsername, saslPassword);
        }
        if (isSsl) {
            this.properties.put(
                    SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                    sslEndpointIdentAlgorithm);
        }

        logger.info("ConnectionService initialised: bootstrap={}, protocol={}", bootstrapServers, securityProtocol);
    }

    // -------------------------------------------------------------------------
    // AdminClient access
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Closes the {@link AdminClient} when the Spring context shuts down,
     * releasing network connections and background threads.
     */
    @PreDestroy
    public void close() {
        if (adminClient != null) {
            logger.info("Closing AdminClient");
            adminClient.close();
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void configureSasl(String mechanism, String username, String password) {
        if (username == null || username.isBlank()) {
            throw new IllegalStateException(
                    "KAFKA_SASL_USERNAME must be set when KAFKA_SECURITY_PROTOCOL is SASL_*");
        }
        if (password == null || password.isBlank()) {
            throw new IllegalStateException(
                    "KAFKA_SASL_PASSWORD must be set when KAFKA_SECURITY_PROTOCOL is SASL_*");
        }

        String loginModule = resolveLoginModule(mechanism);
        String jaasConfig  = String.format(
                "%s required username=\"%s\" password=\"%s\";",
                loginModule, username, password);

        this.properties.put(SaslConfigs.SASL_MECHANISM, mechanism);
        this.properties.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);

        logger.info("SASL configured: mechanism={}", mechanism);
    }

    /**
     * Maps a SASL mechanism name to its Kafka login-module class.
     *
     * @param mechanism one of {@code PLAIN}, {@code SCRAM-SHA-256}, {@code SCRAM-SHA-512}
     * @throws IllegalArgumentException for unsupported mechanisms
     */
    private static String resolveLoginModule(String mechanism) {
        return switch (mechanism.toUpperCase()) {
            case "PLAIN" ->
                    "org.apache.kafka.common.security.plain.PlainLoginModule";
            case "SCRAM-SHA-256", "SCRAM-SHA-512" ->
                    "org.apache.kafka.common.security.scram.ScramLoginModule";
            default -> throw new IllegalArgumentException(
                    "Unsupported SASL mechanism: '" + mechanism +
                    "'. Supported values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512");
        };
    }
}
