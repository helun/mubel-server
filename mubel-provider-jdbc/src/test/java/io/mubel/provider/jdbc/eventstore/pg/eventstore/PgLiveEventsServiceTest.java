package io.mubel.provider.jdbc.eventstore.pg.eventstore;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.eventstore.JdbcEventStore;
import io.mubel.provider.jdbc.eventstore.JdbcEventStoreProvisioner;
import io.mubel.provider.jdbc.eventstore.pg.PgErrorMapper;
import io.mubel.provider.test.LiveEventsServiceTestBase;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executors;

@Testcontainers
class PgLiveEventsServiceTest extends LiveEventsServiceTestBase {

    @Container
    static PostgreSQLContainer container = Containers.postgreSQLContainer();

    static EventStore eventStore;

    static LiveEventsService service;

    @BeforeAll
    static void setup() {

        var dataSource = Containers.dataSource(container);
        String eventStoreName = "test_es";
        JdbcEventStoreProvisioner.provision(dataSource, new PgEventStoreStatements(eventStoreName));
        var jdbi = Jdbi.create(dataSource);
        eventStore = new JdbcEventStore(
                jdbi,
                new PgEventStoreStatements(eventStoreName),
                new PgErrorMapper()
        ).init();
        service = new PgLiveEventsService(
                dataSource,
                PgEventStoreStatements.liveChannelName(eventStoreName),
                (JdbcEventStore) eventStore,
                Schedulers.fromExecutor(Executors.newVirtualThreadPerTaskExecutor())
        );
    }

    @AfterAll
    static void tearDown() {
        ((PgLiveEventsService) service).stop();
    }

    @Override
    protected String esid() {
        return "some_esid";
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