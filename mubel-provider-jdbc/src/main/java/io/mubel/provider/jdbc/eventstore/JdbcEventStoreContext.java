package io.mubel.provider.jdbc.eventstore;

import io.mubel.provider.jdbc.queue.JdbcMessageQueueService;
import io.mubel.server.spi.eventstore.ReplayService;

public record JdbcEventStoreContext(
        JdbcEventStore eventStore,
        JdbcEventStoreProvisioner provisioner,
        ReplayService replayService,
        JdbcLiveEventsService liveEventsService,
        JdbcMessageQueueService messageQueueService
) {
    public void close() {
        liveEventsService.stop();
    }
}
