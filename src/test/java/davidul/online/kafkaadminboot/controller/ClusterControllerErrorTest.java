package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.service.ClusterService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ClusterController.class)
public class ClusterControllerErrorTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    ClusterService clusterService;

    @Test
    void cluster_returns_500_when_service_throws() throws Exception {
        Mockito.when(clusterService.describeCluster())
                .thenThrow(new InternalException(new RuntimeException("Kafka unavailable")));

        this.mockMvc.perform(get("/cluster"))
                .andExpect(status().isInternalServerError());
    }
}

