package io.mubel.provider.jdbc.eventstore.mysql;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.eventstore.JdbcEventStore;
import io.mubel.provider.jdbc.eventstore.JdbcEventStoreProvisioner;
import io.mubel.provider.jdbc.eventstore.PollingLiveEventsService;
import io.mubel.provider.jdbc.support.SqlStatements;
import io.mubel.provider.test.eventstore.LiveEventsServiceTestBase;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executors;

@Testcontainers
public class MysqlLiveEventsServiceTest extends LiveEventsServiceTestBase {

    @Container
    static JdbcDatabaseContainer<?> container = Containers.mySqlContainer();

    static JdbcEventStore eventStore;

    static LiveEventsService service;

    @BeforeAll
    static void setup() {

        var dataSource = Containers.dataSource(container);
        String eventStoreName = "test_es";
        MysqlEventStoreStatements statements = new MysqlEventStoreStatements(eventStoreName);
        JdbcEventStoreProvisioner.provision(dataSource, SqlStatements.of(statements.ddl()));
        var jdbi = Jdbi.create(dataSource);
        eventStore = new JdbcEventStore(
                jdbi,
                statements,
                new MysqlErrorMapper()
        ).init();
        int pollingIntervalMs = 1000;
        service = new PollingLiveEventsService(
                pollingIntervalMs,
                eventStore,
                Schedulers.fromExecutor(Executors.newVirtualThreadPerTaskExecutor())
        );
    }

    @Override
    protected String esid() {
        return "a/esid";
    }

    @Override
    protected EventStore eventStore() {
        return eventStore;
    }

    @Override
    protected LiveEventsService service() {
        return service;
    }
}
