package davidul.online.kafkaadminboot.service;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link ConnectionService} security-configuration logic.
 *
 * These tests instantiate {@code ConnectionService} directly (no Spring context)
 * so they run fast and do not require an embedded broker.
 */
class ConnectionServiceTest {

    // -------------------------------------------------------------------------
    // Helper: extract the private 'properties' field via reflection
    // -------------------------------------------------------------------------
    private static Properties getProperties(ConnectionService service) throws Exception {
        Field field = ConnectionService.class.getDeclaredField("properties");
        field.setAccessible(true);
        return (Properties) field.get(service);
    }

    // -------------------------------------------------------------------------
    // PLAINTEXT (default)
    // -------------------------------------------------------------------------

    @Test
    void plaintext_setsBootstrapAndProtocolOnly() throws Exception {
        ConnectionService service = new ConnectionService(
                "localhost:9092", "PLAINTEXT", "SCRAM-SHA-512", "", "", "https");

        Properties props = getProperties(service);

        assertThat(props.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG))
                .isEqualTo("localhost:9092");
        assertThat(props.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG))
                .isEqualTo("PLAINTEXT");
        assertThat(props.containsKey(SaslConfigs.SASL_MECHANISM)).isFalse();
        assertThat(props.containsKey(SaslConfigs.SASL_JAAS_CONFIG)).isFalse();
        assertThat(props.containsKey(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)).isFalse();
    }

    // -------------------------------------------------------------------------
    // SASL_SSL
    // -------------------------------------------------------------------------

    @Test
    void saslSsl_scramSha512_setsAllSecurityProperties() throws Exception {
        ConnectionService service = new ConnectionService(
                "broker:9093", "SASL_SSL", "SCRAM-SHA-512", "alice", "s3cr3t", "https");

        Properties props = getProperties(service);

        assertThat(props.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG))
                .isEqualTo("SASL_SSL");
        assertThat(props.getProperty(SaslConfigs.SASL_MECHANISM))
                .isEqualTo("SCRAM-SHA-512");
        assertThat(props.getProperty(SaslConfigs.SASL_JAAS_CONFIG))
                .contains("ScramLoginModule")
                .contains("username=\"alice\"")
                .contains("password=\"s3cr3t\"");
        assertThat(props.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG))
                .isEqualTo("https");
    }

    @Test
    void saslSsl_scramSha256_usesScramLoginModule() throws Exception {
        ConnectionService service = new ConnectionService(
                "broker:9093", "SASL_SSL", "SCRAM-SHA-256", "bob", "pass", "https");

        Properties props = getProperties(service);

        assertThat(props.getProperty(SaslConfigs.SASL_JAAS_CONFIG))
                .contains("ScramLoginModule");
    }

    @Test
    void saslSsl_plain_usesPlainLoginModule() throws Exception {
        ConnectionService service = new ConnectionService(
                "broker:9093", "SASL_SSL", "PLAIN", "admin", "admin", "https");

        Properties props = getProperties(service);

        assertThat(props.getProperty(SaslConfigs.SASL_MECHANISM)).isEqualTo("PLAIN");
        assertThat(props.getProperty(SaslConfigs.SASL_JAAS_CONFIG))
                .contains("PlainLoginModule");
    }

    // -------------------------------------------------------------------------
    // SASL_PLAINTEXT (auth without TLS)
    // -------------------------------------------------------------------------

    @Test
    void saslPlaintext_doesNotSetSslProperties() throws Exception {
        ConnectionService service = new ConnectionService(
                "broker:9094", "SASL_PLAINTEXT", "PLAIN", "user", "pass", "https");

        Properties props = getProperties(service);

        assertThat(props.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG))
                .isEqualTo("SASL_PLAINTEXT");
        assertThat(props.getProperty(SaslConfigs.SASL_MECHANISM)).isEqualTo("PLAIN");
        assertThat(props.containsKey(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG)).isFalse();
    }

    // -------------------------------------------------------------------------
    // SSL only (no SASL)
    // -------------------------------------------------------------------------

    @Test
    void ssl_onlySetsSslProperty_noSasl() throws Exception {
        ConnectionService service = new ConnectionService(
                "broker:9093", "SSL", "SCRAM-SHA-512", "", "", "https");

        Properties props = getProperties(service);

        assertThat(props.getProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG))
                .isEqualTo("SSL");
        assertThat(props.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG))
                .isEqualTo("https");
        assertThat(props.containsKey(SaslConfigs.SASL_MECHANISM)).isFalse();
    }

    @Test
    void ssl_emptyEndpointAlgorithm_disablesHostnameVerification() throws Exception {
        ConnectionService service = new ConnectionService(
                "broker:9093", "SSL", "SCRAM-SHA-512", "", "", "");

        Properties props = getProperties(service);

        assertThat(props.getProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG))
                .isEmpty();
    }

    // -------------------------------------------------------------------------
    // Validation errors
    // -------------------------------------------------------------------------

    @Test
    void sasl_missingUsername_throwsIllegalState() {
        assertThatThrownBy(() -> new ConnectionService(
                "broker:9093", "SASL_SSL", "SCRAM-SHA-512", "", "secret", "https"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("KAFKA_SASL_USERNAME");
    }

    @Test
    void sasl_missingPassword_throwsIllegalState() {
        assertThatThrownBy(() -> new ConnectionService(
                "broker:9093", "SASL_SSL", "SCRAM-SHA-512", "alice", "", "https"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("KAFKA_SASL_PASSWORD");
    }

    @Test
    void sasl_unsupportedMechanism_throwsIllegalArgument() {
        assertThatThrownBy(() -> new ConnectionService(
                "broker:9093", "SASL_SSL", "GSSAPI", "alice", "secret", "https"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("GSSAPI")
                .hasMessageContaining("PLAIN, SCRAM-SHA-256, SCRAM-SHA-512");
    }
}

