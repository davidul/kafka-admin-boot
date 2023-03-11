package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.controller.Topics;
import davidul.online.kafkaadminboot.exception.InternalException;
import davidul.online.kafkaadminboot.exception.KafkaTimeoutException;
import davidul.online.kafkaadminboot.model.*;
import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
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
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@PropertySource("classpath:application.properties")
public class TopicService {

    private final ConnectionService connectionService;

    private final KafkaResultQueue kafkaResultQueue;

    @Value("${admin.timeout}")
    private String timeout;

    private static final Logger logger = LoggerFactory.getLogger(TopicService.class);

    public TopicService(ConnectionService connectionService, KafkaResultQueue kafkaResultQueue) {
        this.connectionService = connectionService;
        this.kafkaResultQueue = kafkaResultQueue;
    }

    /**
     * Returns {@link ListTopicsDTO} .
     *
     * @param listInternal internal topics included
     * @return {@link ListTopicsDTO} topic names
     */
    public ListTopicsDTO listTopics(Boolean listInternal) throws InternalException, KafkaTimeoutException {
        logger.debug("Listing topics");
        final ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(listInternal);

        final ListTopicsResult listTopicsResult = connectionService.adminClient().listTopics(listTopicsOptions);
        KafkaFuture<Set<String>> names = listTopicsResult.names();
        Set<String> listTopics = KafkaFutureHandler.handleFuture(names,
                "listTopics",
                kafkaResultQueue,
                Integer.valueOf(timeout));
        ListTopicsDTO listTopicsDTO = new ListTopicsDTO(listTopics, false, null);
        return listTopicsDTO;
    }

    public Map<String, TopicDescription> describeTopicsAll(Boolean internal) throws KafkaTimeoutException, InternalException {
        ListTopicsDTO listTopicsDTO = listTopics(internal);
        KafkaFuture<Map<String, TopicDescription>> mapKafkaFuture = connectionService.adminClient()
                .describeTopics(listTopicsDTO.getTopicNames()).allTopicNames();
        Map<String, TopicDescription> describeTopicsAll = KafkaFutureHandler.handleFuture(mapKafkaFuture,
                "describeTopicsAll",
                kafkaResultQueue,
                Integer.valueOf(timeout));
        return describeTopicsAll;
    }

    /**
     * Describe topic.
     *
     * @param name
     * @return
     */
    public TopicDescription describeTopic(String name) throws InternalException, KafkaTimeoutException {
        DescribeTopicsResult describeTopicsResult = connectionService
                .adminClient().describeTopics(Collections.singletonList(name));
        final Map<String, KafkaFuture<TopicDescription>> topic = describeTopicsResult.topicNameValues();

        try {
            Set<String> names = topic.keySet();
            for (String uuid : names) {
                TopicDescription topicDescription = topic.get(uuid).get(Long.valueOf(timeout), TimeUnit.MILLISECONDS);
                return topicDescription;
            }
            return null;
        } catch (InterruptedException | ExecutionException e) {
            throw new InternalException(e);
        } catch (TimeoutException e) {
            logger.debug("Timeout exception");
            String key = kafkaResultQueue.add(
                    new KafkaRequest(LocalDateTime.now(), topic.get(name), "describeTopic"));

            throw new KafkaTimeoutException(key);
        }
    }

    public String createTopic(String name) throws InternalException, KafkaTimeoutException {
        Set<NewTopic> topics = new HashSet<>();
        NewTopic newTopic = new NewTopic(name, 1, (short) 1);
        topics.add(newTopic);
        CreateTopicsResult topics1 = connectionService.adminClient().createTopics(topics);
        KafkaFuture<Uuid> uuidKafkaFuture = topics1.topicId(name);
        Uuid createTopic = KafkaFutureHandler.handleFuture(uuidKafkaFuture,
                "createTopic",
                kafkaResultQueue,
                Integer.valueOf(timeout));
        return createTopic.toString();
    }

    public void deleteTopic(String name) throws KafkaTimeoutException, InternalException {
        DeleteTopicsResult deleteTopicsResult = connectionService.adminClient()
                .deleteTopics(Collections.singletonList(name));
        KafkaFuture<Void> all = deleteTopicsResult.all();
        Void deleteTopic = KafkaFutureHandler.handleFuture(all,
                "deleteTopic",
                kafkaResultQueue,
                Integer.valueOf(timeout));
    }

    public void createPartition(String topicName, int numPartitions) {
        Map<String, NewPartitions> map = new HashMap<>();
        final NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        map.put(topicName, newPartitions);
        CreatePartitionsResult partitions = connectionService.adminClient().createPartitions(map);

    }

    public void deleteRecords(String topicName, int partition) throws InternalException {
        final TopicPartition topicPartition = new TopicPartition(topicName, partition);
        final HashMap<TopicPartition, RecordsToDelete> deleteHashMap = new HashMap<>();
        deleteHashMap.put(topicPartition, RecordsToDelete.beforeOffset(Integer.MAX_VALUE));
        try {
            connectionService.adminClient().deleteRecords(deleteHashMap).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new InternalException(e);
        }
    }

    public ListOffsetsResult.ListOffsetsResultInfo offset(String topicName, int partition, OffsetSpec offsetSpec) throws InternalException {
        final TopicPartition topicPartition = new TopicPartition(topicName, partition);
        Map<TopicPartition, OffsetSpec> specMap = new HashMap<>();
        specMap.put(topicPartition, offsetSpec);
        try {
            return
                    connectionService.adminClient().listOffsets(specMap).partitionResult(topicPartition).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new InternalException(e);
        }
    }

    public List<ConsumerGroupListingDTO> listConsumerGroups() {
        final ListConsumerGroupsResult listConsumerGroupsResult = connectionService.adminClient().listConsumerGroups();
        List<ConsumerGroupListingDTO> dtos = new ArrayList<>();
        try {
            for (ConsumerGroupListing consumerGroupListing : listConsumerGroupsResult.all().get()) {
                dtos.add(new ConsumerGroupListingDTO(consumerGroupListing.groupId(),
                        consumerGroupListing.isSimpleConsumerGroup()));
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return dtos;
    }

    public Map<TopicPartitionDTO, OffsetAndMetadataDTO> listConsumerGroupOffsets(String groupId) {
        Map<TopicPartitionDTO, OffsetAndMetadataDTO> map = new HashMap<>();
        final ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult = connectionService.adminClient().listConsumerGroupOffsets(groupId);
        try {
            final Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
            for (TopicPartition topicPartition : topicPartitionOffsetAndMetadataMap.keySet()) {
                final OffsetAndMetadata offsetAndMetadata = topicPartitionOffsetAndMetadataMap.get(topicPartition);
                final TopicPartitionDTO topicPartitionDTO = Topics.topicPartition(topicPartition);
                final OffsetAndMetadataDTO offsetAndMetadataDTO = Topics.offsetAndMetadata(offsetAndMetadata);
                map.put(topicPartitionDTO, offsetAndMetadataDTO);
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return map;

    }

    public Map<String, ConsumerGroupDescriptionDTO> describerConsumerGroups(Collection<String> groupIds) {
        Map<String, ConsumerGroupDescriptionDTO> consumerGroupDescriptionDTOMap = new HashMap<>();
        final KafkaFuture<Map<String, ConsumerGroupDescription>> all = connectionService.adminClient().describeConsumerGroups(groupIds).all();
        try {
            final Map<String, ConsumerGroupDescription> map = all.get();
            for (String s : map.keySet()) {
                final ConsumerGroupDescription consumerGroupDescription = map.get(s);
                final ConsumerGroupDescriptionDTO consumerGroupDescriptionDTO = Topics.consumerGroupDescription(consumerGroupDescription);
                consumerGroupDescriptionDTOMap.put(s, consumerGroupDescriptionDTO);
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return consumerGroupDescriptionDTOMap;
    }

    public Map<Integer, Map<String, LogDirInfoDTO>> describeLogDirs(Collection<Integer> brokers) {
        Map<Integer, Map<String, LogDirInfoDTO>> brokerMap = new HashMap<>();

        final DescribeLogDirsResult describeLogDirsResult = connectionService.adminClient().describeLogDirs(brokers);
        try {
            final Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> integerMapMap = describeLogDirsResult.all().get();
            for (Integer integer : integerMapMap.keySet()) {
                final Map<String, DescribeLogDirsResponse.LogDirInfo> stringLogDirInfoMap = integerMapMap.get(integer);
                Map<String, LogDirInfoDTO> dirInfoDTOMap = new HashMap<>();
                for (String s : stringLogDirInfoMap.keySet()) {
                    final DescribeLogDirsResponse.LogDirInfo logDirInfo = stringLogDirInfoMap.get(s);
                    final LogDirInfoDTO logDirInfoDTO = Topics.logDirInfo(logDirInfo);
                    dirInfoDTOMap.put(s, logDirInfoDTO);
                }
                brokerMap.put(integer, dirInfoDTOMap);

            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return brokerMap;

    }


    public void describeConfigs() {

    }

    public void fetcher() {

    }

    public void x(Collection<Integer> brokers) {
    }

    public Object handleFuture(KafkaFuture kafkaFuture, String createdBy) throws InternalException, KafkaTimeoutException {
        try {
            return kafkaFuture.get(Integer.parseInt(timeout), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Exception: ", e);
            throw new InternalException(e);
        } catch (TimeoutException e) {
            logger.debug("Timeout exception");
            String key = kafkaResultQueue.add(
                    new KafkaRequest(LocalDateTime.now(), kafkaFuture, createdBy));

            throw new KafkaTimeoutException(key);
        }
    }
}
