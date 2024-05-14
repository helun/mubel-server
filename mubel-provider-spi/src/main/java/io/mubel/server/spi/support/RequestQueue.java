package io.mubel.server.spi.support;

import io.mubel.server.spi.execute.AsyncRequestHandler;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RequestQueue<T, E> implements AsyncRequestHandler<T, E> {

    private final BlockingQueue<Entry<T, E>> queue;
    private final int timeoutMillis;

    public RequestQueue(int capacity, int timeoutMillis) {
        queue = new ArrayBlockingQueue<>(capacity);
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public CompletableFuture<E> submit(T request) {
        final var future = new CompletableFuture<E>();
        try {
            if (!queue.offer(new Entry<>(request, future), timeoutMillis, TimeUnit.MILLISECONDS)) {
                return CompletableFuture.failedFuture(new RuntimeException("Queue is full"));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }
        return future;
    }

    public Entry<T, E> poll() {
        return queue.poll();
    }

    public Entry<T, E> take() throws InterruptedException {
        return queue.take();
    }

    public int size() {
        return queue.size();
    }

    public record Entry<T, E>(T request, CompletableFuture<E> future) {
    }
}
