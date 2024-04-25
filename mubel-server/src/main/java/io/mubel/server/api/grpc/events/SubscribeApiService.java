package io.mubel.server.api.grpc.events;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.v1.events.Deadline;
import io.mubel.api.grpc.v1.events.DeadlineSubscribeRequest;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import io.mubel.server.api.grpc.validation.Validators;
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
        var validated = Validators.validate(request);
        Flux<EventData> stream = eventStoreManager.subscribe(validated);
        stream.subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
    }

    public void subcribeToDeadlines(DeadlineSubscribeRequest request, StreamObserver<Deadline> responseObserver) {
        Flux<Deadline> stream = eventStoreManager.subcribeToDeadlines(request);
        stream.subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
    }
}
