package io.mubel.provider.inmemory.eventstore;

import io.mubel.provider.test.eventstore.EventStoreTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

class InMemEventStoreTest extends EventStoreTestBase {

    @BeforeAll
    static void start() {
        eventStore = new InMemEventStore();
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