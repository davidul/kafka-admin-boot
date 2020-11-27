package davidul.basic;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

    private KeyValueStore<K,V> stateStore;

    @Override
    public void init(ProcessorContext processorContext) {
        stateStore = (KeyValueStore<K,V>)processorContext.getStateStore("my-store");
    }

    @Override
    public KeyValue<K, V> transform(K k, V v) {
        System.out.println("Tramsform " + k);
        final V v1 = stateStore.get(k);
        if(v1 == null) {
            stateStore.put(k, v);
            return KeyValue.pair(k, v);
        }
        else
            return null;
    }

    @Override
    public void close() {

    }
}
