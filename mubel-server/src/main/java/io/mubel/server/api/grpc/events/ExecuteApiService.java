package io.mubel.server.api.grpc.events;

import io.mubel.api.grpc.v1.events.ExecuteRequest;
import io.mubel.server.api.grpc.validation.Validators;
import io.mubel.server.eventstore.EventStoreManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;

@Service
public class ExecuteApiService {

    private final EventStoreManager eventStoreManager;

    public ExecuteApiService(EventStoreManager eventStoreManager) {
        this.eventStoreManager = eventStoreManager;
    }

    @Transactional
    public CompletableFuture<Void> execute(ExecuteRequest request) {
        var validated = Validators.validate(request);
        var ctx = eventStoreManager.eventStoreContext(validated.getEsid());
        return ctx.executeRequestHandler().handle(request);
    }

}
