package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.v1.events.ExecuteRequest;

import java.util.concurrent.CompletableFuture;

public interface ExecuteRequestHandler {

    CompletableFuture<Void> handle(ExecuteRequest request);

    default void stop() {
    }
}
