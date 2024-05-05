package io.mubel.server.spi;

import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.ExecuteRequestHandler;
import io.mubel.server.spi.eventstore.LiveEventsService;
import io.mubel.server.spi.eventstore.ReplayService;
import io.mubel.server.spi.queue.MessageQueueService;

public record EventStoreContext(
        String esid,
        ExecuteRequestHandler executeRequestHandler,
        EventStore eventStore,
        ReplayService replayService,
        LiveEventsService liveEventsService,
        MessageQueueService scheduledEventsQueue
) {
}
