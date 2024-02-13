package io.mubel.server.spi.queue;

import java.time.Duration;

public record ReceiveRequest(
        String queueName,
        Duration timeout
) {
}
