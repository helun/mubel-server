package io.mubel.provider.jdbc.topic;

import reactor.core.publisher.Flux;

public interface Topic {
    void publish(String message);

    Flux<String> consumer();

    int consumerCount();
}
