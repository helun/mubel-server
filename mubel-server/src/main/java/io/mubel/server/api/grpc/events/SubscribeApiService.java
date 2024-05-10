package io.mubel.server.api.grpc.events;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.v1.events.Deadline;
import io.mubel.api.grpc.v1.events.DeadlineSubscribeRequest;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import io.mubel.server.api.grpc.GrpcExceptionAdvice;
import io.mubel.server.api.grpc.validation.Validators;
import io.mubel.server.eventstore.EventStoreManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class SubscribeApiService {

    private static final Logger LOG = LoggerFactory.getLogger(SubscribeApiService.class);

    private final EventStoreManager eventStoreManager;
    private GrpcExceptionAdvice grpcExceptionAdvice;

    public SubscribeApiService(EventStoreManager eventStoreManager, GrpcExceptionAdvice grpcExceptionAdvice) {
        this.eventStoreManager = eventStoreManager;
        this.grpcExceptionAdvice = grpcExceptionAdvice;
    }

    public void subscribe(SubscribeRequest request, StreamObserver<EventData> responseObserver) {
        var validated = Validators.validate(request);
        eventStoreManager.subscribe(validated)
                .doOnSubscribe(sub -> LOG.debug("Started sub for {}", request.getEsid()))
                .doOnError(err -> LOG.error("Error in sub for {}", request.getEsid(), err))
                .doOnComplete(() -> LOG.debug("Completed sub for {}", request.getEsid()))
                .subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
    }

    public void subcribeToDeadlines(DeadlineSubscribeRequest request, StreamObserver<Deadline> responseObserver) {
        eventStoreManager.subcribeToDeadlines(request)
                .onErrorMap(grpcExceptionAdvice::handleException)
                .subscribe(responseObserver::onNext, responseObserver::onError, responseObserver::onCompleted);
    }
}
