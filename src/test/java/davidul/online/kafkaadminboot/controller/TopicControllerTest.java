package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.service.ConnectionService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.ResultMatcher;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"admin.timeout=10000"},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
//@EmbeddedKafka(partitions = 1,
//        brokerProperties = {"listeners=INTERNAL://localhost:9092",
//                "port=9092",
//                //"advertised.listeners=EXTERNAL://localhost:9093",
//                "listener.security.protocol.map=INTERNAL:PLAINTEXT",
//                "inter.broker.listener.name=INTERNAL",
//                "bootstrap.servers=localhost:9092",
//                "auto.create.topics.enable=${kafka.broker.topics-enable:true}"},
//        topics = {"test-topic"})
class TopicControllerTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    private ConnectionService connectionService;

    @BeforeAll
    static void beforeAll() {
        EmbeddedKafkaZKBroker embeddedKafkaZKBroker = new EmbeddedKafkaZKBroker(1);

        embeddedKafkaZKBroker.kafkaPorts(9092);
        embeddedKafkaZKBroker.afterPropertiesSet();
        embeddedKafkaZKBroker.getKafkaServer(0).startup();
        embeddedKafkaZKBroker.addTopics("test-topic");
    }

    @Test
    void getTopics() throws Exception {
        this.mockMvc.perform(get("/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name", is("test-topic")));
        // .andDo(MockMvcResultHandlers.print());
    }

    @Test
    void describeTopics() throws Exception {
        this.mockMvc.perform(get("/topics/describe"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name", is("test-topic")));
    }

    @Test
    void describeTopic() throws Exception {
        ResultMatcher resultMatcher = result -> {
            int status = result.getResponse().getStatus();
            if (status == 202) {
                status().is5xxServerError();
            }
            if (status == 200) {
                status().isOk();
            }

            if (status > 399) {
                status().is3xxRedirection();
            }
        };

        this.mockMvc.perform(get("/topic/describe/{name}", "test-topic"))
                .andDo(result -> {
                    int status = result.getResponse().getStatus();
                    if (status > 202)
                        throw new AssertionError("Error");

                    result.getResponse().getHeader("queue-id");
                });

    }

    @Test
    void createTopic() throws Exception {
        this.mockMvc.perform(post("/topic/{name}", "test-topic-a"))
                .andExpect(status().isAccepted());
    }

    @Test
    void addPartition() throws Exception {
        this.mockMvc.perform(post("/topic/{name}/partition/{count}", "test-topic", 1))
                .andExpect(status().isAccepted());
    }

    @Test
    void deleteTopic() throws Exception {
        this.mockMvc.perform(post("/topic/{name}", "test-topic-b"))
                .andExpect(status().isAccepted());

        this.mockMvc.perform(delete("/topic/{name}", "test-topic-b"))
                .andExpect(status().isAccepted());
    }

    @Test
    void deleteRecords() {
    }
}