package io.mubel.provider.test;

import io.mubel.server.spi.DataStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;

public class TestSubscriber<E> {


    private final DataStream<E> dataStream;
    private final List<E> values = new ArrayList<>();
    private Throwable error;

    public TestSubscriber(DataStream<E> dataStream) {
        this.dataStream = dataStream;
        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                while (true) {
                    final var next = dataStream.next();
                    if (next == null) {
                        return;
                    }
                    values.add(next);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                error = e;
            }
        });
    }

    public TestSubscriber<E> awaitDone() {
        await().untilAsserted(() -> {
            if (dataStream.isDone()) {
                return;
            }
            throw new AssertionError("DataStream is not done");
        });
        return this;
    }

    public TestSubscriber<E> assertComplete() {
        if (!dataStream.isDone()) {
            throw new AssertionError("DataStream is not done");
        }
        return this;
    }

    public TestSubscriber<E> assertNoErrors() {
        if (error != null) {
            throw new AssertionError("DataStream has error", error);
        }
        return this;
    }

    public TestSubscriber<E> assertValueCount(int count) {
        if (values.size() != count) {
            throw new AssertionError("DataStream has " + values.size() + " values, expected " + count);
        }
        return this;
    }

    public TestSubscriber<E> awaitCount(int count) {
        await().untilAsserted(() -> {
            if (values.size() == count) {
                return;
            }
            throw new AssertionError("DataStream has " + values.size() + " values, expected " + count);
        });
        return this;
    }

    public List<E> values() {
        return values;
    }

    public TestSubscriber<E> assertNoValues() {
        if (!values.isEmpty()) {
            throw new AssertionError("DataStream has values");
        }
        return this;
    }
}
