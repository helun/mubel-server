package io.mubel.server.configuration;

import io.mubel.server.spi.queue.QueueConfiguration;
import io.mubel.server.spi.queue.QueueConfigurations;
import io.mubel.server.spi.support.IdGenerator;
import io.mubel.server.support.DefaultIdGenerator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@EnableConfigurationProperties(MubelConfigurationProperties.class)
public class ApplicationConfiguration {

    @Bean
    public IdGenerator idGenerator() {
        return DefaultIdGenerator.defaultGenerator();
    }

    @Bean
    public QueueConfiguration scheduledEventsQueueConfig(MubelConfigurationProperties properties) {
        return new QueueConfiguration(
                "deadlines",
                properties.deadlines().visibility().timeout(),
                properties.deadlines().polling().interval()
        );
    }

    @Bean
    public QueueConfigurations queueConfigurations(List<QueueConfiguration> queueConfigs) {
        return new QueueConfigurations(queueConfigs);
    }

}
