package io.mubel.provider.jdbc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class JdbcProviderTestApplication {
    public static void main(String[] args) {
        SpringApplication.run(JdbcProviderTestApplication.class, args);
    }

}
