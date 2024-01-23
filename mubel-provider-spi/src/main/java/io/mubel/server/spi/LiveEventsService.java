package io.mubel.server.spi;

import io.mubel.api.grpc.EventData;

public interface LiveEventsService {

    DataStream<EventData> liveEvents();
    
}
