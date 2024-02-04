package io.mubel.provider.inmemory.eventstore;

import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.api.grpc.SubscribeRequest;
import io.mubel.server.spi.eventstore.ReplayService;
import reactor.core.publisher.Flux;

import static java.util.Objects.requireNonNull;

public class InMemReplayService implements ReplayService {

    private final InMemEventStores eventStores;

    public InMemReplayService(InMemEventStores eventStores) {
        this.eventStores = requireNonNull(eventStores);
    }

    @Override
    public Flux<EventData> replay(SubscribeRequest request) {
        final var es = eventStores.get(request.getEsid());
        final var response = es.get(GetEventsRequest.newBuilder()
                .setEsid(request.getEsid())
                .setFromSequenceNo(request.getFromSequenceNo())
                .setSize(0)
                .build());
        return Flux.fromIterable(response.getEventList());
    }
}
