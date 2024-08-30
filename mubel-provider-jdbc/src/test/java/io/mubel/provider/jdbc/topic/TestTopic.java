package io.mubel.provider.jdbc.topic;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicInteger;

public class TestTopic implements Topic {

    private final AtomicInteger consumerCount = new AtomicInteger();
    private final Sinks.Many<String> sink = Sinks.many().multicast().directBestEffort();

    @Override
    public void publish(String message) {
        sink.tryEmitNext(message);
    }

    @Override
    public Flux<String> consumer() {
        return sink.asFlux()
                .doOnSubscribe(subscription -> consumerCount.incrementAndGet())
                .doFinally(signalType -> consumerCount.decrementAndGet());
    }

    @Override
    public int consumerCount() {
        return consumerCount.get();
    }
}
