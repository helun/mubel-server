package io.mubel.server.spi.queue;

import java.util.UUID;

public record Message(
        UUID messageId,
        String queueName,
        String type,
        byte[] payload
) {
}
