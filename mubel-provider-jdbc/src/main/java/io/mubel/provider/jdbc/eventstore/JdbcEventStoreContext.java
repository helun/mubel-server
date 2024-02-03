package io.mubel.provider.jdbc.eventstore;

import io.mubel.server.spi.eventstore.ReplayService;

public record JdbcEventStoreContext(
        JdbcEventStore eventStore,
        JdbcEventStoreProvisioner provisioner,
        ReplayService replayService,
        JdbcLiveEventsService liveEventsService
) {
    public void close() {
        liveEventsService.stop();
    }
}
