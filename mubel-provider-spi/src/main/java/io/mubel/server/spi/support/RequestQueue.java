/*
 * mubel-provider-spi - Multi Backend Event Log
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
