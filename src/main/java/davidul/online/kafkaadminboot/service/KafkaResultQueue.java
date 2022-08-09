package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaResultQueue<T> {
    private Map<String, KafkaRequest<T>> mp;

    public KafkaResultQueue(){
        this.mp = new ConcurrentHashMap<>();
    }

    public String add(KafkaRequest<T> kafkaFuture){
        UUID uuid = UUID.randomUUID();
        mp.put(uuid.toString(), kafkaFuture);
        return uuid.toString();
    }

    public KafkaRequest<T> get(String uuid){
        return mp.get(uuid);
    }

    public KafkaRequest<T> remove(String key){
        return mp.remove(key);
    }

    public Set<String> keys(){
        return mp.keySet();
    }
}
