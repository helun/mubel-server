package io.mubel.provider.jdbc;

import io.mubel.server.spi.groups.GroupsProperties;
import io.mubel.server.spi.queue.QueueConfiguration;
import io.mubel.server.spi.queue.QueueConfigurations;
import io.mubel.server.spi.support.IdGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.List;

@SpringBootApplication
@EnableCaching
public class JdbcProviderTestApplication {
    public static void main(String[] args) {
        SpringApplication.run(JdbcProviderTestApplication.class, args);
    }

    @Bean
    public IdGenerator idGenerator() {
        return new IdGenerator() {
        };
    }

    @Bean
    public QueueConfigurations queueConfigurations() {
        return new QueueConfigurations(List.of(
                new QueueConfiguration("deadlines", Duration.ofMinutes(1), Duration.ofMillis(500))
        ));
    }

    @Bean
    public GroupsProperties groupsProperties() {
        return new GroupsProperties(Duration.ofSeconds(2));
    }

}
