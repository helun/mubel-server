package io.mubel.server.spi.messages;

import io.mubel.server.spi.EventStoreContext;

public sealed interface EventStoreEvents {

    record EventStoreOpened(String esid,
                            EventStoreContext context
    ) implements EventStoreEvents {
    }

    record EventStoreClosed(String esid) implements EventStoreEvents {
    }

}
