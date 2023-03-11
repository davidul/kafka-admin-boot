package davidul.online.kafkaadminboot.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"admin.timeout=10000"},
        webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"},
        topics = {"test-topic"})
class TopicControllerTest {

    @Autowired
    MockMvc mockMvc;

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
        this.mockMvc.perform(get("/topic/describe/{name}", "test-topic"))
                .andExpect(status().isOk());
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