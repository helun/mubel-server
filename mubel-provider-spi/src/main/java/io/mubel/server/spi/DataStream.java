package io.mubel.server.spi;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DataStream<E> {

    private final BlockingQueue<E> buffer;
    private final E endMessage;
    private AtomicReference<RuntimeException> error = new AtomicReference<>();
    private AtomicBoolean completed = new AtomicBoolean(false);

    public DataStream(int bufferSize, E endMessage) {
        this.buffer = new ArrayBlockingQueue<>(bufferSize);
        this.endMessage = endMessage;
    }

    public E next() throws InterruptedException {
        if (completed.get()) {
            return null;
        }
        final E v = buffer.take();
        if (v == endMessage) {
            final var t = error.get();
            if (t != null) {
                throw t;
            }
            return null;
        }
        return v;
    }

    public void put(E e) throws InterruptedException {
        buffer.put(e);
    }

    public void fail(RuntimeException t) {
        error.set(t);
        buffer.clear();
        end();
    }

    public void end() {
        completed.set(true);
        buffer.add(endMessage);
    }

    public boolean isDone() {
        return completed.get();
    }
}
