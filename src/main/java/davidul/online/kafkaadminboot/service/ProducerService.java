package davidul.online.kafkaadminboot.service;

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
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }


    public void produce(String topic, String message){
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerProperties(""));
        final ProducerRecord<String, String> objectStringProducerRecord = new ProducerRecord<>(topic, message);
        kafkaProducer.send(objectStringProducerRecord, (metadata, exception) -> {
            System.out.println("Offset " + metadata.offset());
        });
    }

}
