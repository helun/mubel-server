package io.mubel.provider.jdbc;

import io.mubel.server.spi.support.IdGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;

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

}
