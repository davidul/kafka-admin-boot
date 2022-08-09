package davidul.online.kafkaadminboot;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
public class KafkaAdminBootApplication {


    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminBootApplication.class, args);
    }

    @Bean
    WebMvcConfigurer corsConfigurer(){
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**").allowedOrigins("http://localhost:3000").allowedHeaders("*");
            }
        };
    }
}
