package davidul.online.kafkaadminboot.controller;

import davidul.online.kafkaadminboot.model.FutureDescriptionDTO;
import davidul.online.kafkaadminboot.model.internal.KafkaRequest;
import davidul.online.kafkaadminboot.service.KafkaResultQueue;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@RestController
public class KafkaQueueController {

    private final KafkaResultQueue kafkaResultQueue;

    public KafkaQueueController(KafkaResultQueue kafkaResultQueue) {
        this.kafkaResultQueue = kafkaResultQueue;
    }


    @GetMapping(value = "/kafka/{uuid}")
    public ResponseEntity<FutureDescriptionDTO> getQueue(@PathVariable("uuid") String uuid) {
        KafkaRequest voidKafkaFuture = this.kafkaResultQueue.get(uuid);
        if(voidKafkaFuture != null) {
            boolean done = voidKafkaFuture.isDone();
            boolean cancelled = voidKafkaFuture.isCancelled();
            boolean completedExceptionally = voidKafkaFuture.isCompletedExceptionally();
            LocalDateTime createdAt = voidKafkaFuture.getCreatedAt();
            Class type = voidKafkaFuture.getType();

            if(done){
                try {
                    Object o = voidKafkaFuture.getKafkaFuture().get();
                    this.kafkaResultQueue.remove(uuid);
                    return ResponseEntity.ok(
                            new FutureDescriptionDTO(String.valueOf(done),
                                    String.valueOf(cancelled),
                                    String.valueOf(completedExceptionally),
                                    createdAt,
                                    o != null ? o.getClass().toString() : "null"));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            return ResponseEntity.ok(
                    new FutureDescriptionDTO(String.valueOf(done),
                            String.valueOf(cancelled),
                            String.valueOf(completedExceptionally),
                            createdAt,
                            type.toString()));
        }else {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping(value = "/kafka")
    public ResponseEntity<Set> getAll(){
        Set keys = this.kafkaResultQueue.keys();
        return ResponseEntity.ok(keys);
    }

    public void getFutureValue(){

    }

}
