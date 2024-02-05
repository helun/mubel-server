package io.mubel.provider.jdbc.eventstore.mysql;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.eventstore.JdbcEventStore;
import io.mubel.provider.jdbc.eventstore.JdbcEventStoreProvisioner;
import io.mubel.provider.test.eventstore.EventStoreTestBase;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class MysqlEventStoreTest extends EventStoreTestBase {

    @Container
    static JdbcDatabaseContainer<?> container = Containers.mySqlContainer();

    @BeforeAll
    static void setup() {
        var dataSource = Containers.dataSource(container);
        String eventStoreName = "test_es";
        MysqlEventStoreStatements statements = new MysqlEventStoreStatements(eventStoreName);
        JdbcEventStoreProvisioner.provision(dataSource, statements);
        eventStore = new JdbcEventStore(
                Jdbi.create(dataSource),
                statements,
                new MysqlErrorMapper()
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
