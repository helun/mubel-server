package io.mubel.server.api.grpc.events;

import io.mubel.api.grpc.v1.events.GetEventsRequest;
import io.mubel.api.grpc.v1.events.GetEventsResponse;
import io.mubel.server.api.grpc.Validator;
import io.mubel.server.eventstore.EventStoreManager;
import org.springframework.stereotype.Service;

@Service
public class EventApiService {

    private final EventStoreManager eventStoreManager;

    public EventApiService(EventStoreManager eventStoreManager) {
        this.eventStoreManager = eventStoreManager;
    }

    public GetEventsResponse get(GetEventsRequest request) {
        final var validated = Validator.validate(request);
        return eventStoreManager.resolveEventStore(validated.getEsid())
                .get(validated);
    }
}
