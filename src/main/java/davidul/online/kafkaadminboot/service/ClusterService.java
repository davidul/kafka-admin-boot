package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.controller.Topics;
import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.model.ClusterDTO;
import davidul.online.kafkaadminboot.model.NodeDTO;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Service
public class ClusterService {

    private final ConnectionService connectionService;

    private static final Logger logger = LoggerFactory.getLogger(ClusterService.class);

    public ClusterService(ConnectionService connectionService) {
        this.connectionService = connectionService;
    }

    public ClusterDTO describeCluster() throws InternalException {
        logger.info("describe cluster");
        List<NodeDTO> nodeDTOList = new ArrayList<>();
        final DescribeClusterResult describeClusterResult = connectionService.adminClient().describeCluster();
        try {
            for (Node node : describeClusterResult.nodes().get()) {
                nodeDTOList.add(Topics.node(node));
            }
            final String clusterId = describeClusterResult.clusterId().get();
            final Node controllerNode = describeClusterResult.controller().get();
            final NodeDTO controller = Topics.node(controllerNode);
            return new ClusterDTO(clusterId, nodeDTOList, controller);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while describing cluster", e);
            throw new InternalException(e);
        } catch (ExecutionException e) {
            logger.error("Exception while describing cluster", e);
            throw new InternalException(e);
        }
    }
}
