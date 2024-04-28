package davidul.online.kafkaadminboot.controller;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"admin.timeout=10000"},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
public class ClusterControllerTest {

    @Autowired
    MockMvc mockMvc;

    @BeforeAll
    static void beforeAll() {
        EmbeddedKafkaZKBroker embeddedKafkaZKBroker = new EmbeddedKafkaZKBroker(1);

        embeddedKafkaZKBroker.kafkaPorts(9092);
        embeddedKafkaZKBroker.afterPropertiesSet();
        embeddedKafkaZKBroker.getKafkaServer(0).startup();
        embeddedKafkaZKBroker.addTopics("test-topic");
    }

    @Test
    void cluster() throws Exception {
        this.mockMvc.perform(get("/cluster"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterId", Matchers.notNullValue()));
    }
}
