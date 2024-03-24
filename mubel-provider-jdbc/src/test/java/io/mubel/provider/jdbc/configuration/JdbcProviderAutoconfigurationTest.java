package io.mubel.provider.jdbc.configuration;

import io.mubel.api.grpc.DataFormat;
import io.mubel.provider.jdbc.JdbcProviderTestApplication;
import io.mubel.provider.jdbc.eventstore.EventStoreFactory;
import io.mubel.provider.jdbc.support.JdbcDataSources;
import io.mubel.server.spi.model.BackendType;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import io.mubel.server.spi.systemdb.JobStatusRepository;
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

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(classes = JdbcProviderTestApplication.class)
@TestPropertySource("classpath:application-test-properties")
class JdbcProviderAutoconfigurationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:latest");

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("mubel.provider.jdbc.datasources[0].name", () -> "pg_backend");
        registry.add("mubel.provider.jdbc.datasources[0].url", postgres::getJdbcUrl);
        registry.add("mubel.provider.jdbc.datasources[0].username", postgres::getUsername);
        registry.add("mubel.provider.jdbc.datasources[0].password", postgres::getPassword);

        registry.add("mubel.provider.jdbc.datasources[1].name", () -> "systemdb");
        registry.add("mubel.provider.jdbc.datasources[1].url", postgres::getJdbcUrl);
        registry.add("mubel.provider.jdbc.datasources[1].username", postgres::getUsername);
        registry.add("mubel.provider.jdbc.datasources[1].password", postgres::getPassword);

        registry.add("mubel.provider.jdbc.datasources[2].name", () -> "mysql_backend");
        registry.add("mubel.provider.jdbc.datasources[2].url", mysql::getJdbcUrl);
        registry.add("mubel.provider.jdbc.datasources[2].username", mysql::getUsername);
        registry.add("mubel.provider.jdbc.datasources[2].password", mysql::getPassword);
    }

    @Autowired
    JdbcProviderProperties properties;

    @Autowired
    JdbcDataSources dataSources;

    @Autowired
    EventStoreFactory eventStoreFactory;

    @Autowired
    EventStoreDetailsRepository detailsRepository;

    @Autowired
    JobStatusRepository jobStatusRepository;

    @Test
    void baseCase() {
        assertThat(properties.isEnabled()).isTrue();
        assertThat(dataSources.get("pg_backend").backendType()).isEqualTo(BackendType.PG);
        assertThat(dataSources.get("mysql_backend").backendType()).isEqualTo(BackendType.MYSQL);
        assertThat(dataSources.get("systemdb").backendType()).isEqualTo(BackendType.PG);
    }

    @Test
    void createPostgresEventStore() {
        var command = new ProvisionCommand(
                UUID.randomUUID().toString(),
                "test-esid",
                DataFormat.JSON,
                "postgres"
        );
        var context = eventStoreFactory.create(command);
        assertThat(context).isNotNull();
        assertThat(context.eventStore()).isNotNull();
        assertThat(context.provisioner()).isNotNull();
        assertThat(context.liveEventsService()).isNotNull();
        assertThat(context.replayService()).isNotNull();
    }
}