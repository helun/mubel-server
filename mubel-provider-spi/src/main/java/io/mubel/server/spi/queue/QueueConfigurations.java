package io.mubel.server.spi.queue;

import io.mubel.server.spi.exceptions.ResourceNotFoundException;

import java.util.List;
import java.util.Optional;

public record QueueConfigurations(
        List<QueueConfiguration> queues
) {

    public QueueConfiguration getQueue(String name, String defaults) {
        return findConfig(name)
                .or(() -> findConfig(defaults))
                .map(config -> {
                    if (name.equals(config.name())) {
                        return config;
                    }
                    return new QueueConfiguration(name,
                            config.visibilityTimeout(),
                            config.polIInterval()
                    );
                })
                .orElseThrow(() -> new ResourceNotFoundException("Queue not found: " + name));
    }

    private Optional<QueueConfiguration> findConfig(String name) {
        return queues.stream()
                .filter(queue -> queue.name().equals(name))
                .findFirst();
    }

}
