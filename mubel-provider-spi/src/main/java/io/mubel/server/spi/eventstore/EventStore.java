package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.v1.events.AppendOperation;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.GetEventsRequest;
import io.mubel.api.grpc.v1.events.GetEventsResponse;
import io.mubel.api.grpc.v1.server.EventStoreSummary;

import java.util.List;

public interface EventStore {

    List<EventData> append(AppendOperation operation);

    GetEventsResponse get(GetEventsRequest request);

    void truncate();

    EventStoreSummary summary();

}
