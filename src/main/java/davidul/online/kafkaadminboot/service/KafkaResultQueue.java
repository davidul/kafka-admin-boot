package davidul.online.kafkaadminboot.service;

import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaResultQueue {

    private Map<String, KafkaRequest<?>> mp;

    public KafkaResultQueue(){
        this.mp = new ConcurrentHashMap<>();
    }

    public String add(KafkaRequest<?> kafkaFuture){
        UUID uuid = UUID.randomUUID();
        mp.put(uuid.toString(), kafkaFuture);
        return uuid.toString();
    }

    public KafkaRequest<?> get(String uuid){
        return mp.get(uuid);
    }

    public KafkaRequest<?> remove(String key){
        return mp.remove(key);
    }

    public Set<String> keys(){
        return mp.keySet();
    }
}
