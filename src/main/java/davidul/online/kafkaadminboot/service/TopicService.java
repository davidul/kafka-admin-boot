package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.controller.Topics;
import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.*;
import davidul.online.kafkaadminboot.model.internal.ListTopicsDTO;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class TopicService {

    private final ConnectionService connectionService;
    private final KafkaResultQueue kafkaResultQueue;
    private final int timeout;

    private static final Logger logger = LoggerFactory.getLogger(TopicService.class);

    public TopicService(ConnectionService connectionService,
                        KafkaResultQueue kafkaResultQueue,
                        @Value("${admin.timeout}") int timeout) {
        this.connectionService = connectionService;
        this.kafkaResultQueue = kafkaResultQueue;
        this.timeout = timeout;
    }

    // -------------------------------------------------------------------------
    // Topic operations
    // -------------------------------------------------------------------------

    /**
     * Returns the set of topic names visible to the current principal.
     *
     * @param listInternal include internal Kafka topics (e.g. __consumer_offsets)
     */
    public ListTopicsDTO listTopics(Boolean listInternal) throws InternalException, KafkaTimeoutException {
        logger.debug("Listing topics");
        final ListTopicsOptions options = new ListTopicsOptions().listInternal(listInternal);
        final KafkaFuture<Set<String>> names = connectionService.adminClient().listTopics(options).names();
        return new ListTopicsDTO(handleFuture(names, "listTopics"), false, null);
    }

    /**
     * Describes all topics, optionally including internal ones.
     */
    public Map<String, TopicDescription> describeTopicsAll(Boolean internal)
            throws KafkaTimeoutException, InternalException {
        ListTopicsDTO listTopicsDTO = listTopics(internal);
        KafkaFuture<Map<String, TopicDescription>> future = connectionService.adminClient()
                .describeTopics(listTopicsDTO.getTopicNames()).allTopicNames();
        return handleFuture(future, "describeTopicsAll");
    }

    /**
     * Describes a single topic by name.
     */
    public TopicDescription describeTopic(String name) throws InternalException, KafkaTimeoutException {
        final KafkaFuture<TopicDescription> future = connectionService.adminClient()
                .describeTopics(Collections.singletonList(name))
                .topicNameValues()
                .get(name);
        return handleFuture(future, "describeTopic");
    }

    /**
     * Creates a topic with 1 partition and replication factor 1.
     *
     * @return the new topic's UUID string
     */
    public String createTopic(String name) throws InternalException, KafkaTimeoutException {
        NewTopic newTopic = new NewTopic(name, 1, (short) 1);
        CreateTopicsResult result = connectionService.adminClient().createTopics(Set.of(newTopic));
        Uuid uuid = handleFuture(result.topicId(name), "createTopic");
        return uuid.toString();
    }

    /**
     * Deletes the named topic.
     */
    public void deleteTopic(String name) throws KafkaTimeoutException, InternalException {
        KafkaFuture<Void> future = connectionService.adminClient()
                .deleteTopics(Collections.singletonList(name)).all();
        handleFuture(future, "deleteTopic");
    }

    /**
     * Sets the total partition count for a topic.
     * Note: {@code numPartitions} is the desired <em>total</em>, not an increment.
     */
    public void createPartition(String topicName, int numPartitions)
            throws InternalException, KafkaTimeoutException {
        Map<String, NewPartitions> map = Map.of(topicName, NewPartitions.increaseTo(numPartitions));
        handleFuture(connectionService.adminClient().createPartitions(map).all(), "createPartition");
    }

    /**
     * Deletes all records in the given partition (truncates to latest offset).
     */
    public void deleteRecords(String topicName, int partition)
            throws InternalException, KafkaTimeoutException {
        TopicPartition topicPartition = new TopicPartition(topicName, partition);
        Map<TopicPartition, RecordsToDelete> deleteMap =
                Map.of(topicPartition, RecordsToDelete.beforeOffset(Long.MAX_VALUE));
        handleFuture(connectionService.adminClient().deleteRecords(deleteMap).all(), "deleteRecords");
    }

    /**
     * Returns offset information for the given partition and offset spec.
     */
    public ListOffsetsResult.ListOffsetsResultInfo offset(String topicName, int partition,
                                                          OffsetSpec offsetSpec)
            throws InternalException, KafkaTimeoutException {
        TopicPartition topicPartition = new TopicPartition(topicName, partition);
        Map<TopicPartition, OffsetSpec> specMap = Map.of(topicPartition, offsetSpec);
        return handleFuture(
                connectionService.adminClient().listOffsets(specMap).partitionResult(topicPartition),
                "offset");
    }

    // -------------------------------------------------------------------------
    // Consumer group operations
    // -------------------------------------------------------------------------

    public List<ConsumerGroupListingDTO> listConsumerGroups()
            throws InternalException, KafkaTimeoutException {
        Collection<ConsumerGroupListing> listings = handleFuture(
                connectionService.adminClient().listConsumerGroups().all(),
                "listConsumerGroups");
        List<ConsumerGroupListingDTO> dtos = new ArrayList<>();
        for (ConsumerGroupListing listing : listings) {
            dtos.add(new ConsumerGroupListingDTO(listing.groupId(), listing.isSimpleConsumerGroup()));
        }
        return dtos;
    }

    public Map<TopicPartitionDTO, OffsetAndMetadataDTO> listConsumerGroupOffsets(String groupId)
            throws InternalException, KafkaTimeoutException {
        Map<TopicPartition, OffsetAndMetadata> raw = handleFuture(
                connectionService.adminClient()
                        .listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
                "listConsumerGroupOffsets");
        Map<TopicPartitionDTO, OffsetAndMetadataDTO> result = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : raw.entrySet()) {
            result.put(Topics.topicPartition(entry.getKey()), Topics.offsetAndMetadata(entry.getValue()));
        }
        return result;
    }

    public Map<String, ConsumerGroupDescriptionDTO> describerConsumerGroups(Collection<String> groupIds)
            throws InternalException, KafkaTimeoutException {
        Map<String, ConsumerGroupDescription> raw = handleFuture(
                connectionService.adminClient().describeConsumerGroups(groupIds).all(),
                "describerConsumerGroups");
        Map<String, ConsumerGroupDescriptionDTO> result = new HashMap<>();
        for (Map.Entry<String, ConsumerGroupDescription> entry : raw.entrySet()) {
            result.put(entry.getKey(), Topics.consumerGroupDescription(entry.getValue()));
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Log directory operations
    // -------------------------------------------------------------------------

    public Map<Integer, Map<String, LogDirInfoDTO>> describeLogDirs(Collection<Integer> brokers)
            throws InternalException, KafkaTimeoutException {
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> raw = handleFuture(
                connectionService.adminClient().describeLogDirs(brokers).all(),
                "describeLogDirs");
        Map<Integer, Map<String, LogDirInfoDTO>> brokerMap = new HashMap<>();
        for (Map.Entry<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> brokerEntry : raw.entrySet()) {
            Map<String, LogDirInfoDTO> dirMap = new HashMap<>();
            for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> dirEntry : brokerEntry.getValue().entrySet()) {
                dirMap.put(dirEntry.getKey(), Topics.logDirInfo(dirEntry.getValue()));
            }
            brokerMap.put(brokerEntry.getKey(), dirMap);
        }
        return brokerMap;
    }

    // -------------------------------------------------------------------------
    // Future handling
    // -------------------------------------------------------------------------

    /**
     * Convenience wrapper that delegates to {@link KafkaFutureHandler} with
     * the injected {@link KafkaResultQueue} and {@code timeout}.
     */
    public <T> T handleFuture(KafkaFuture<T> kafkaFuture, String createdBy)
            throws InternalException, KafkaTimeoutException {
        return KafkaFutureHandler.handleFuture(kafkaFuture, createdBy, kafkaResultQueue, timeout);
    }
}
