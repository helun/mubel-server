package io.mubel.server.spi.eventstore;


import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import reactor.core.publisher.Flux;

public interface ReplayService {

    Flux<EventData> replay(SubscribeRequest request);

}
