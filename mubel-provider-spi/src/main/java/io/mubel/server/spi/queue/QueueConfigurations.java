package io.mubel.server.spi.queue;

import io.mubel.server.spi.exceptions.ResourceNotFoundException;

import java.util.List;

public record QueueConfigurations(
        List<QueueConfiguration> queues
) {

    public QueueConfiguration getQueue(String name) {
        return queues.stream()
                .filter(queue -> queue.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new ResourceNotFoundException("Queue not found: " + name));
    }

}
