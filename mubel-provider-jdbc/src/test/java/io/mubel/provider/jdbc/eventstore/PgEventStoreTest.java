package io.mubel.provider.jdbc.eventstore;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.eventstore.pg.PgErrorMapper;
import io.mubel.provider.jdbc.eventstore.pg.PgEventStoreStatements;
import io.mubel.provider.test.EventStoreTestBase;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class PgEventStoreTest extends EventStoreTestBase {

    @Container
    static PostgreSQLContainer container = Containers.postgreSQLContainer();

    @BeforeAll
    static void setup() {
        var dataSource = Containers.dataSource(container);
        String eventStoreName = "test_es";
        JdbcEventStoreProvisioner.provision(dataSource, new PgEventStoreStatements(eventStoreName));
        eventStore = new JdbcEventStore(
                Jdbi.create(dataSource),
                new PgEventStoreStatements(eventStoreName),
                new PgErrorMapper()
        ).init();
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
