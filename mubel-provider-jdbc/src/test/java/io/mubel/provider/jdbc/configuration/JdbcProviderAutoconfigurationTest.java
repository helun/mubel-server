package io.mubel.provider.jdbc.configuration;

import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.provider.jdbc.JdbcProviderTestApplication;
import io.mubel.provider.jdbc.eventstore.EventStoreFactory;
import io.mubel.provider.jdbc.eventstore.configuration.JdbcProviderProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(classes = JdbcProviderTestApplication.class)
@TestPropertySource("classpath:application-test-properties")
class JdbcProviderAutoconfigurationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Container
    static MySQLContainer mysql = new MySQLContainer<>("mysql:latest");

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("mubel.provider.jdbc.datasources.pg_backend.url", postgres::getJdbcUrl);
        registry.add("mubel.provider.jdbc.datasources.pg_backend.username", postgres::getUsername);
        registry.add("mubel.provider.jdbc.datasources.pg_backend.password", postgres::getPassword);

        registry.add("mubel.provider.jdbc.datasources.mysql_backend.url", mysql::getJdbcUrl);
        registry.add("mubel.provider.jdbc.datasources.mysql_backend.username", mysql::getUsername);
        registry.add("mubel.provider.jdbc.datasources.mysql_backend.password", mysql::getPassword);
    }

    @Autowired
    JdbcProviderProperties properties;

    @Autowired
    Map<String, DataSource> dataSources;

    @Autowired
    EventStoreFactory eventStoreFactory;

    @Test
    void baseCase() {
        assertThat(properties.isEnabled()).isTrue();
        assertThat(dataSources).containsKeys("pg_backend", "mysql_backend");
    }

    @Test
    void createPostgresEventStore() {
        var request = ProvisionEventStoreRequest.newBuilder()
                .setEsid("test-esid")
                .setStorageBackendName("pg_backend")
                .build();
        var context = eventStoreFactory.create(request);
        assertThat(context).isNotNull();
        assertThat(context.eventStore()).isNotNull();
        assertThat(context.provisioner()).isNotNull();
        assertThat(context.liveEventsService()).isNotNull();
        assertThat(context.replayService()).isNotNull();
    }
}