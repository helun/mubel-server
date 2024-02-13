package io.mubel.server.configuration;

import io.mubel.server.spi.support.IdGenerator;
import io.mubel.server.support.DefaultIdGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfiguration {

    @Bean
    public IdGenerator idGenerator() {
        return DefaultIdGenerator.defaultGenerator();
    }

}
