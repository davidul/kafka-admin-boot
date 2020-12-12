package davidul.basic;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;

public class StreamSample {

    public void doit(String bootstrap, String topic){
        final Gson gson = new Gson();
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> stream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));
        stream
                .foreach((s1, s2) -> System.out.println("Foreach: Key = " + s1 + " Value: " + s2));

        final KStream<String, String> created = stream
                .filter((k, v) -> gson.fromJson(v, ReactorProducer.Event.class).getType().equalsIgnoreCase("CREATE"));
        created.to("second_topic");
        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), Config.streamProperties(bootstrap));
        kafkaStreams.start();

        Stores.inMemoryKeyValueStore("aaa");
        //kafkaStreams.store()
        //QueryableStoreType
    }
}
