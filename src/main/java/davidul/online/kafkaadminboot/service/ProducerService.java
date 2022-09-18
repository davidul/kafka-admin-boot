package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.model.RecordMetadataDTO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class ProducerService {

    public static Properties producerProperties(String bootstrap){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_BOOTSTRAP"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }


    public void produce(String topic, String message){
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties(""));
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message);
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            new RecordMetadataDTO(metadata.offset(),
                    metadata.timestamp(),
                    metadata.serializedKeySize(),
                    metadata.serializedValueSize(),
                    null);
        });
    }

}
