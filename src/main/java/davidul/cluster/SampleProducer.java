package davidul.cluster;

import com.github.javafaker.Faker;
import davidul.basic.Config;
import io.vertx.core.AbstractVerticle;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

/**
 *
 * @author ulicny.david@gmail.com
 */
public class SampleProducer extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleProducer.class);

    private final KafkaProducer<String, String> kafkaProducer;

    private final Faker faker;

    @Override
    public void start() throws Exception {
        Flux
                .range(1, 10)
                .map(i ->
                    Tuples.of(i.toString(), new TrekMessage(faker.starTrek().location(), faker.starTrek().specie(), faker.starTrek().character()).toString())
        ).subscribe(t -> produce(t.getT1(), t.getT2()));

    }

    public SampleProducer(String bootstrap){
        this.faker = new Faker();
        this.kafkaProducer = new KafkaProducer<>(Config.producerProperties(bootstrap));
    }

    public void produce(String key, String message){
        ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", key, message);
        this.kafkaProducer.send(record, (c, e) -> {
           LOGGER.info("partition: " + c.partition() + " , offset: " + c.offset());
        });
    }

}
