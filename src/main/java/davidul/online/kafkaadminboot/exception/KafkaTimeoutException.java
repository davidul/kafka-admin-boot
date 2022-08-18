package davidul.online.kafkaadminboot.exception;

public class KafkaTimeoutException extends Throwable {

    private String key;

    public KafkaTimeoutException(String key){
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
