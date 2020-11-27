package davidul.basic;

import io.vavr.collection.List;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.LocalDateTime;

public class ReactorProducer {

    public Flux<Tuple2<Integer, Event>> produceEvents(int max){
        return Flux.range(1, max)
                .delayElements(Duration.ofMillis(100))
                .map(integer -> Tuples.of(integer, getEvent(integer)));
    }

    public List<Event> getEvents(int max){
        return List.range(1, max).map(this::getEvent);
    }

    public Event getEvent(Integer i){
        final List<String> of = List.of("CREATE", "DELETE", "UPDATE");
        final Event event = new Event();
        event.setCreated(LocalDateTime.now().toString());
        event.setKey(i.toString());
        event.setType(of.shuffle().get(0));
        return event;
    }

    static class Event{
        String type;
        String created;
        String key;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getCreated() {
            return created;
        }

        public void setCreated(String created) {
            this.created = created;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }
}
