package io.mubel.provider.inmemory;

import io.mubel.server.spi.queue.QueueConfiguration;
import io.mubel.server.spi.queue.QueueConfigurations;
import io.mubel.server.spi.support.IdGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.List;

@SpringBootApplication
public class InMemProviderTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(InMemProviderTestApplication.class, args);
    }

    @Bean
    public QueueConfigurations queueConfigurations() {
        return new QueueConfigurations(List.of(
                new QueueConfiguration("scheduledEvents", Duration.ofSeconds(30), Duration.ofMillis(500))
        ));
    }

    @Bean
    public IdGenerator idGenerator() {
        return new IdGenerator() {
        };
    }
}
