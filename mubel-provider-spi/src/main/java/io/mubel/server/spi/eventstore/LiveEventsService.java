package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.EventData;
import io.mubel.server.spi.DataStream;

public interface LiveEventsService {

    DataStream<EventData> liveEvents();

}
