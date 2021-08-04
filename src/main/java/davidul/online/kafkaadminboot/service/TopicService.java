package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.controller.Topics;
import davidul.online.kafkaadminboot.model.ConsumerGroupListingDTO;
import davidul.online.kafkaadminboot.model.OffsetAndMetadataDTO;
import davidul.online.kafkaadminboot.model.TopicPartitionDTO;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Service
public class TopicService {

    private final ConnectionService connectionService;

    public TopicService(ConnectionService connectionService) {
        this.connectionService = connectionService;
    }

    public Set<String> listTopics(){
        final ListTopicsResult listTopicsResult = connectionService.adminClient().listTopics();
        try {
            return listTopicsResult.names().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, TopicDescription> describeTopicsAll(){
        try {
            return connectionService.adminClient().describeTopics(listTopics()).all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public TopicDescription describeTopic(String name){
        try {
            final Map<String, TopicDescription> topic = connectionService.adminClient().describeTopics(Collections.singletonList(name)).all().get();
            return topic.get(name);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void createTopic(String name){
        Set<NewTopic> topics = new HashSet<>();
        NewTopic newTopic = new NewTopic(name, 1, (short) 1);
        topics.add(newTopic);
        try {
            connectionService.adminClient().createTopics(topics).all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void deleteTopic(String name){
        connectionService.adminClient().deleteTopics(Collections.singletonList(name)).all();
    }

    public void createPartition(String topicName, int numPartitions){
        Map<String, NewPartitions> map = new HashMap<>();
        final NewPartitions newPartitions = NewPartitions.increaseTo(2);
        map.put(topicName, newPartitions);
        connectionService.adminClient().createPartitions(map);
    }

    public void deleteRecords(String topicName, int partition){
        final TopicPartition topicPartition = new TopicPartition(topicName, partition);
        final HashMap<TopicPartition, RecordsToDelete> deleteHashMap = new HashMap<>();
        deleteHashMap.put(topicPartition, RecordsToDelete.beforeOffset(Integer.MAX_VALUE));
        try {
            connectionService.adminClient().deleteRecords(deleteHashMap).all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public ListOffsetsResult.ListOffsetsResultInfo offset(String topicName, int partition, OffsetSpec offsetSpec){
        final TopicPartition topicPartition = new TopicPartition(topicName, partition);
        Map<TopicPartition, OffsetSpec> specMap = new HashMap<>();
        specMap.put(topicPartition, offsetSpec);
        try {
            return
                    connectionService.adminClient().listOffsets(specMap).partitionResult(topicPartition).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<ConsumerGroupListingDTO> listConsumerGroups(){
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

    public Map<TopicPartitionDTO, OffsetAndMetadataDTO> listConsumerGroupOffsets(String groupId){
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

    public void describerConsumerGroups(Collection<String> groupIds){
        final KafkaFuture<Map<String, ConsumerGroupDescription>> all = connectionService.adminClient().describeConsumerGroups(groupIds).all();
        try {
            final Map<String, ConsumerGroupDescription> stringConsumerGroupDescriptionMap = all.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
