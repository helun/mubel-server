package io.mubel.server.spi.support;

import java.util.concurrent.CompletableFuture;

public interface AsyncRequestHandler<T, E> {

    CompletableFuture<E> submit(T request);

}
