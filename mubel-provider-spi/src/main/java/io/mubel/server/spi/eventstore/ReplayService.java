package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.SubscribeRequest;
import reactor.core.publisher.Flux;

public interface ReplayService {

    Flux<EventData> replay(SubscribeRequest request);

}
