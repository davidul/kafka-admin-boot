package davidul.online.kafkaadminboot.model;

public record ConsumerGroupOffsetDTO(TopicPartitionDTO partitionDTO, OffsetAndMetadataDTO offsetAndMetadataDTO) {
}
