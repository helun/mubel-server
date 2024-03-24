package io.mubel.server.api.grpc;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.ScheduledEventsSubscribeRequest;
import io.mubel.api.grpc.SubscribeRequest;
import io.mubel.api.grpc.TriggeredEvents;
import io.mubel.server.eventstore.EventStoreManager;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class SubscribeApiService {

    private final EventStoreManager eventStoreManager;

    public SubscribeApiService(EventStoreManager eventStoreManager) {
        this.eventStoreManager = eventStoreManager;
    }

    public void subscribe(SubscribeRequest request, StreamObserver<EventData> responseObserver) {
        Flux<EventData> stream = eventStoreManager.subscribe(request);
        stream.subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
    }

    public void subscribeToScheduledEvents(ScheduledEventsSubscribeRequest request, StreamObserver<TriggeredEvents> responseObserver) {
        Flux<TriggeredEvents> stream = eventStoreManager.subscribeToScheduledEvents(request);
        stream.subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
    }
}
