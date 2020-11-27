package davidul.basic;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;

public class StreamWithStore {


    public void doit(String bootstrap, String topic){
        RocksDbKeyValueBytesStoreSupplier rocksDbKeyValueBytesStoreSupplier = new RocksDbKeyValueBytesStoreSupplier("my-store", false);
        final StoreBuilder storeBuilder = new KeyValueStoreBuilder(rocksDbKeyValueBytesStoreSupplier, Serdes.String(), Serdes.String(), new SystemTime());// InMemoryTimeOrderedKeyValueBuffer.Builder("my-store", Serdes.String(), Serdes.String());

        final Gson gson = new Gson();
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.addStateStore(storeBuilder);

        final KStream<String, String> stream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));
        stream.foreach((s1, s2) -> System.out.println("Foreach: Key = " + s1 + " Value: " + s2));
                stream
                        .transform(() -> new DeduplicationTransformer<>(), "my-store")
                        .filter((k, v) -> gson.fromJson(v, ReactorProducer.Event.class).getType().equalsIgnoreCase("CREATE"))
                        .to("second_topic");
        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), Config.streamProperties(bootstrap));

        kafkaStreams.start();
        //final ReadOnlyKeyValueStore<String, String> store = kafkaStreams.store(StoreQueryParameters.fromNameAndType("my-store", QueryableStoreTypes.keyValueStore()));

        //System.out.println(store.get("1"));
        //kafkaStreams.store()
        //QueryableStoreType
    }
}
