package io.mubel.server.api.grpc.events;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.v1.events.Deadline;
import io.mubel.api.grpc.v1.events.DeadlineSubscribeRequest;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import io.mubel.server.api.grpc.GrpcExceptionAdvice;
import io.mubel.server.api.grpc.validation.Validators;
import io.mubel.server.eventstore.EventStoreManager;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class SubscribeApiService {

    private final EventStoreManager eventStoreManager;
    private GrpcExceptionAdvice grpcExceptionAdvice;

    public SubscribeApiService(EventStoreManager eventStoreManager, GrpcExceptionAdvice grpcExceptionAdvice) {
        this.eventStoreManager = eventStoreManager;
        this.grpcExceptionAdvice = grpcExceptionAdvice;
    }

    public void subscribe(SubscribeRequest request, StreamObserver<EventData> responseObserver) {
        var validated = Validators.validate(request);
        Flux<EventData> stream = eventStoreManager.subscribe(validated);
        stream.subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
    }

    public void subcribeToDeadlines(DeadlineSubscribeRequest request, StreamObserver<Deadline> responseObserver) {
        eventStoreManager.subcribeToDeadlines(request)
                .onErrorMap(grpcExceptionAdvice::handleException)
                .subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
    }
}
