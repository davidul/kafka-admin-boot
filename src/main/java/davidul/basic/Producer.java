package davidul.basic;

import io.vavr.collection.List;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

    private final KafkaProducer<String, String> kafkaProducer;

    public Producer(String bootstrap){
        this.kafkaProducer = new KafkaProducer<>(Config.producerProperties(bootstrap));
    }

    public void produce(List<ProducerRecord<String, String>> records){
        records.forEach(r -> kafkaProducer.send(r,
                (recordMetadata, e) -> System.out.println("Offset " + recordMetadata.offset() + " , partition " + recordMetadata.partition())));
        kafkaProducer.flush();
        kafkaProducer.close();
    }

}
