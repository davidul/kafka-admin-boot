package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.controller.Topics;
import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.ClusterDTO;
import davidul.online.kafkaadminboot.model.NodeDTO;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeFeaturesResult;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Service
public class ClusterService {

    private final ConnectionService connectionService;

    private final TopicService topicService;

    @Value("${admin.timeout}")
    private String timeout;

    private static final Logger logger = LoggerFactory.getLogger(TopicService.class);

    public ClusterService(ConnectionService connectionService, TopicService topicService) {
        this.connectionService = connectionService;
        this.topicService = topicService;
    }

    public ClusterDTO describeCluster() {
        logger.info("describe cluster");
        List<NodeDTO> nodeDTOList = new ArrayList<>();
        final DescribeClusterResult describeClusterResult = connectionService.adminClient().describeCluster();
        try {
            for (Node node : describeClusterResult.nodes().get()) {
                nodeDTOList.add(Topics.node(node));
            }

            final String s = describeClusterResult.clusterId().get();

            final Node node = describeClusterResult.controller().get();
            final NodeDTO controller = Topics.node(node);
            return new ClusterDTO(s, nodeDTOList, controller);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void x() {
        DescribeClientQuotasResult describeClientQuotasResult = connectionService
                .adminClient().describeClientQuotas(ClientQuotaFilter.all());
    }

    public void partition() {
        connectionService.adminClient().listPartitionReassignments();
    }

    public void featureMetadata() throws KafkaTimeoutException, InternalException {
        KafkaFutureHandler
                .handleFuture(
                        connectionService
                                .adminClient()
                                .describeFeatures().featureMetadata(), "", null,
                        Integer.valueOf(timeout));

        DescribeFeaturesResult describeFeaturesResult = connectionService
                .adminClient()
                .describeFeatures();

        FeatureMetadata describeFeature = (FeatureMetadata) topicService
                .handleFuture(describeFeaturesResult.featureMetadata(), "describeFeature");
    }
}
