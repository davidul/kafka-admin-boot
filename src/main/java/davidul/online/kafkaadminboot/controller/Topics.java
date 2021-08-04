package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.model.ConsumerGroupDescriptionDTO;
import davidul.online.kafkaadminboot.model.MemberDescriptionDTO;
import davidul.online.kafkaadminboot.model.NodeDTO;
import davidul.online.kafkaadminboot.model.OffsetAndMetadataDTO;
import davidul.online.kafkaadminboot.model.PartitionInfoDTO;
import davidul.online.kafkaadminboot.model.TopicPartitionDTO;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Topics {

    public static List<PartitionInfoDTO> partitions(List<TopicPartitionInfo> partitionInfos){
        return partitionInfos
                .stream()
                .map(p -> new PartitionInfoDTO(p.partition(), p.leader().toString()))
                .collect(Collectors.toList());
    }

    public static TopicPartitionDTO topicPartition(TopicPartition topicPartition){
        return new TopicPartitionDTO(topicPartition.topic(), topicPartition.partition());
    }

    public static Set<TopicPartitionDTO> topicPartitionSet(Set<TopicPartition> topicPartitions){
        return topicPartitions.stream().map(tp -> topicPartition(tp)).collect(Collectors.toSet());
    }


    public static OffsetAndMetadataDTO offsetAndMetadata(OffsetAndMetadata offsetAndMetadata){
        return new OffsetAndMetadataDTO(offsetAndMetadata.offset(), offsetAndMetadata.metadata());
    }

    public static ConsumerGroupDescriptionDTO consumerGroupDescription(ConsumerGroupDescription description){
        return new ConsumerGroupDescriptionDTO(description.groupId(),
                description.isSimpleConsumerGroup(),
                memberDescriptionList(description.members()),
                description.partitionAssignor(),
                description.state().name(),
                node(description.coordinator()));
    }

    public static MemberDescriptionDTO memberDescription(MemberDescription memberDescription){
        return new MemberDescriptionDTO(memberDescription.consumerId(),
                memberDescription.groupInstanceId().orElse(""),
                memberDescription.clientId(),
                memberDescription.host(),
                topicPartitionSet(memberDescription.assignment().topicPartitions()));
    }

    public static List<MemberDescriptionDTO> memberDescriptionList(Collection<MemberDescription> memberDescriptions){
        return memberDescriptions.stream().map(m -> memberDescription(m)).collect(Collectors.toList());
    }

    public static NodeDTO node(Node node){
        return new NodeDTO(node.id(), node.host(), node.port(), node.rack());
    }
}
