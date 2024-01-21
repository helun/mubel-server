package io.mubel.provider.jdbc.eventstore;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.eventstore.pg.PgErrorMapper;
import io.mubel.provider.jdbc.eventstore.pg.PgEventStoreStatements;
import io.mubel.provider.test.ReplayServiceTestBase;
import io.mubel.server.spi.EventStore;
import io.mubel.server.spi.ReplayService;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.Executors;

@Testcontainers
public class PgReplayServiceTest extends ReplayServiceTestBase {

    @Container
    static PostgreSQLContainer container = Containers.postgreSQLContainer();

    static EventStore eventStore;

    static ReplayService replayService;

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
        replayService = new JdbcReplayService(jdbi,
                new PgEventStoreStatements(eventStoreName),
                Executors.newVirtualThreadPerTaskExecutor()
        );
    }

    @AfterEach
    void tearDown() {
        eventStore.truncate();
    }

    @Override
    protected String esid() {
        return "some-esid";
    }

    @Override
    protected EventStore eventStore() {
        return eventStore;
    }

    @Override
    protected ReplayService service() {
        return replayService;
    }
}
