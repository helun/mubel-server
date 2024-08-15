package io.mubel.server;

import io.mubel.provider.inmemory.groups.InMemGroupManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;

import java.time.Clock;
import java.time.Duration;

@TestConfiguration(proxyBeanMethods = false)
@EnableAsync
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }

    @Bean
    public InMemGroupManager inMemGroupManager() {
        return new InMemGroupManager(Clock.systemUTC(), Duration.ofSeconds(1));
    }

}
