package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.api.grpc.GetEventsResponse;

import java.util.List;

public interface EventStore {
    List<EventData> append(AppendRequest request);

    GetEventsResponse get(GetEventsRequest request);

    void truncate();

}
