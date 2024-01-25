package io.mubel.server.spi.messages;

import io.mubel.server.spi.EventStoreContext;

public sealed interface MubelEvents {

    record EventStoreOpened(String esid,
                            EventStoreContext context
    ) implements MubelEvents {
    }

    record EventStoreClosed(String esid) implements MubelEvents {
    }

}
