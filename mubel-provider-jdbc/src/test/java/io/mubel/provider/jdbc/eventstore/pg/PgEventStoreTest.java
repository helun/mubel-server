package io.mubel.provider.jdbc.eventstore.pg;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.eventstore.JdbcEventStore;
import io.mubel.provider.jdbc.eventstore.JdbcEventStoreProvisioner;
import io.mubel.provider.jdbc.support.SqlStatements;
import io.mubel.provider.test.eventstore.EventStoreTestBase;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class PgEventStoreTest extends EventStoreTestBase {

    @Container
    static PostgreSQLContainer<?> container = Containers.postgreSQLContainer();

    @BeforeAll
    static void setup() {
        var dataSource = Containers.dataSource(container);
        String eventStoreName = "test_es";
        JdbcEventStoreProvisioner.provision(dataSource, SqlStatements.of(new PgEventStoreStatements(eventStoreName).ddl()));
        eventStore = new JdbcEventStore(
                Jdbi.create(dataSource),
                new PgEventStoreStatements(eventStoreName),
                new PgErrorMapper()
        ).init();
    }

    @Test
    void maxSequenceNo_returns_0_when_no_events_exists() {
        assertThat(((JdbcEventStore) eventStore).maxSequenceNo())
                .isEqualTo(0);
    }

    @AfterEach
    void tearDown() {
        eventStore.truncate();
    }

    @Override
    protected String esid() {
        return "some-esid";
    }
}
