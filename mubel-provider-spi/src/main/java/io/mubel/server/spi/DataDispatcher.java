package io.mubel.server.spi;

import io.mubel.server.spi.exceptions.SlowConsumerException;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataDispatcher<E> {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(DataDispatcher.class);
    private final E endMessage;
    private final int bufferSize;
    private final List<DataStream<E>> consumers = new CopyOnWriteArrayList<>();

    public static <E> Builder<E> builder() {
        return new Builder<>();
    }

    private DataDispatcher(E endMessage, int bufferSize) {
        this.endMessage = endMessage;
        this.bufferSize = bufferSize;
    }

    public DataStream<E> newConsumer() {
        final var ds = new DataStream<E>(bufferSize, endMessage);
        consumers.add(ds);
        return ds;
    }

    public void dispatch(E data) throws InterruptedException {
        if (consumers.size() == 1) {
            consumers.getFirst().put(data);
        } else {
            for (var consumer : consumers) {
                if (consumer.isDone()) {
                    consumers.remove(consumer);
                    LOG.debug("Removing completed consumer");
                } else if (!consumer.offer(data)) {
                    LOG.warn("Consumer buffer full, removing consumer");
                    consumers.remove(consumer);
                    consumer.fail(new SlowConsumerException());
                }
            }
        }
    }

    public void end() {
        for (var consumer : consumers) {
            consumer.end();
        }
    }

    public void fail(RuntimeException t) {
        for (var consumer : consumers) {
            consumer.fail(t);
        }
    }

    public static class Builder<E> {
        private E endMessage = null;
        private int bufferSize = 512;

        public Builder<E> withEndMessage(E endMessage) {
            this.endMessage = endMessage;
            return this;
        }

        public Builder<E> withBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        // build
        public DataDispatcher<E> build() {
            if (endMessage == null) {
                throw new IllegalArgumentException("End message must be set");
            }
            if (bufferSize < 1) {
                throw new IllegalArgumentException("Buffer size must be greater than 0");
            }
            return new DataDispatcher<>(endMessage, bufferSize);
        }
    }
}
