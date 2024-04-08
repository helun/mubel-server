package io.mubel.provider.inmemory.eventstore;

import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.provider.test.eventstore.LiveEventsServiceTestBase;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import io.mubel.server.spi.model.ProvisionCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.util.Set;

public class InMemLiveServiceTest extends LiveEventsServiceTestBase {

    public static final String TEST_ESID = "some-esid";

    static InMemEventStore eventStore;

    static InMemEventStores eventStores = new InMemEventStores(Set.of("in-memory"));

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
    protected LiveEventsService service() {
        return eventStore;
    }
}
