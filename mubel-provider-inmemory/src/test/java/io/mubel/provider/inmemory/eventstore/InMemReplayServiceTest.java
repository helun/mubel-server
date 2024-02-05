package io.mubel.provider.inmemory.eventstore;

import io.mubel.api.grpc.DataFormat;
import io.mubel.provider.test.eventstore.ReplayServiceTestBase;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.ReplayService;
import io.mubel.server.spi.model.ProvisionCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.util.Set;

class InMemReplayServiceTest extends ReplayServiceTestBase {

    public static final String TEST_ESID = "test-esid";
    static EventStore eventStore;

    static InMemEventStores eventStores = new InMemEventStores(Set.of("in-memory"));

    static ReplayService replayService = new InMemReplayService(eventStores);

    @BeforeAll
    static void start() {
        var details = eventStores.provision(new ProvisionCommand(
                "jobid",
                TEST_ESID,
                DataFormat.JSON,
                "in-memory"
        ));
        eventStore = eventStores.create(details);
    }

    @AfterEach
    void tearDown() {
        eventStore.truncate();
    }

    @Override
    protected String esid() {
        return TEST_ESID;
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