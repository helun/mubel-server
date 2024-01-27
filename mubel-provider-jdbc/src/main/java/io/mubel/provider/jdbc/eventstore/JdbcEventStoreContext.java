package io.mubel.provider.jdbc.eventstore;

import io.mubel.server.spi.eventstore.LiveEventsService;
import io.mubel.server.spi.eventstore.ReplayService;

public record JdbcEventStoreContext(
        JdbcEventStore eventStore,
        JdbcEventStoreProvisioner provisioner,
        ReplayService replayService,
        LiveEventsService liveEventsService
) {
}
