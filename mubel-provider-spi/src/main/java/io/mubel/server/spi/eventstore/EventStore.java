package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.*;

import java.util.List;

public interface EventStore {
    List<EventData> append(AppendRequest request);

    GetEventsResponse get(GetEventsRequest request);

    void truncate();

    EventStoreSummary summary();

}
