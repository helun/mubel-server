package io.mubel.server.spi;

import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import io.mubel.server.spi.eventstore.ReplayService;

public record EventStoreContext(
        String esid,
        EventStore eventStore,
        ReplayService replayService,
        LiveEventsService liveEventsService
) {
}
