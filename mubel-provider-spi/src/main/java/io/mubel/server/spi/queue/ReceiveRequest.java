package io.mubel.server.spi.queue;

import java.time.Duration;

public record ReceiveRequest(
        String queueName,
        Duration timeout,
        int maxMessages
) {

    public static final int DEFAULT_MAX_MESSAGES = 16;

    public ReceiveRequest(String queueName,
                          Duration timeout) {
        this(queueName, timeout, DEFAULT_MAX_MESSAGES);
    }
}
