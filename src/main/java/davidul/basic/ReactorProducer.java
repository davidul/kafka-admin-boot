package davidul.basic;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import io.vavr.collection.List;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Producing sample events
 */
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
        event.setCreated(LocalDateTime.now());
        event.setKey(i.toString());
        event.setType(of.shuffle().get(0));
        event.setEventAddress(address());
        return event;
    }

    public EventAddress address(){
        final Address address = Faker.instance().address();
        return new EventAddress(address.city(), address.state(), address.streetAddress(), address.country(), address.firstName(), address.lastName());
    }

    static class Event{
        String type;
        LocalDateTime created;
        LocalDateTime updated;
        String key;
        EventAddress eventAddress;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public LocalDateTime getCreated() {
            return created;
        }

        public void setCreated(LocalDateTime created) {
            this.created = created;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public LocalDateTime getUpdated() {
            return updated;
        }

        public void setUpdated(LocalDateTime updated) {
            this.updated = updated;
        }

        public EventAddress getEventAddress() {
            return eventAddress;
        }

        public void setEventAddress(EventAddress eventAddress) {
            this.eventAddress = eventAddress;
        }
    }

    static class EventAddress {
        private String city;
        private String state;
        private String streetAddress;
        private String country;
        private String firstName;
        private String lastName;

        public EventAddress(String city, String state, String streetAdress, String country, String firstName, String lastName) {
            this.city = city;
            this.state = state;
            this.streetAddress = streetAdress;
            this.country = country;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getStreetAddress() {
            return streetAddress;
        }

        public void setStreetAddress(String streetAddress) {
            this.streetAddress = streetAddress;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }
    }
}
