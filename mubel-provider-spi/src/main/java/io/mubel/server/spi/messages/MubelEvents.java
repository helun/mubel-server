package io.mubel.server.spi.messages;

import io.mubel.server.spi.eventstore.EventStore;

public sealed interface MubelEvents {

    record EventStoreOpened(String esid, EventStore eventStore) implements MubelEvents {
    }

    record EventStoreClosed(String esid) implements MubelEvents {
    }

}
