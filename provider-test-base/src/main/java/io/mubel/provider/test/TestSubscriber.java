package io.mubel.provider.test;

import io.mubel.server.spi.DataStream;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

import static org.awaitility.Awaitility.await;

public class TestSubscriber<E> {

    private final Flux<E> dataStream;
    private final List<E> values = new ArrayList<>();
    private Throwable error;

    private volatile boolean done = false;

    public TestSubscriber(DataStream<E> ds) {
        dataStream = Flux.empty();
    }

    public TestSubscriber(Flux<E> dataStream) {
        this.dataStream = dataStream;
        dataStream.subscribe(
                values::add,
                err -> error = err,
                () -> done = true
        );
    }

    public TestSubscriber<E> awaitDone() {
        await().untilAsserted(() -> {
            if (done) {
                return;
            }
            throw new AssertionError("DataStream is not done");
        });
        return this;
    }

    public TestSubscriber<E> assertComplete() {
        if (!done) {
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
