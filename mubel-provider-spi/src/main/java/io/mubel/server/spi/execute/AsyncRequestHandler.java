package io.mubel.server.spi.execute;

import java.util.concurrent.CompletableFuture;

public interface AsyncRequestHandler<T, E> {

    CompletableFuture<E> submit(T request);

}
