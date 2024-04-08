package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.v1.events.EventData;
import reactor.core.publisher.Flux;

public interface LiveEventsService {

    Flux<EventData> liveEvents();

}
