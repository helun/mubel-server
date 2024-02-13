package io.mubel.server.spi.queue;

import java.time.Duration;

public record QueueConfiguration(
        String name,
        Duration visibilityTimeout
) {


}
