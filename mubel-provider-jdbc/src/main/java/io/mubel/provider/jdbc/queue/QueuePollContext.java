package io.mubel.provider.jdbc.queue;

import io.mubel.server.spi.queue.Message;
import org.jdbi.v3.core.Jdbi;
import reactor.core.publisher.FluxSink;

import java.time.Duration;

public class QueuePollContext {

    private final Jdbi jdbi;
    private final String queueName;

    private final FluxSink<Message> sink;
    private int messageLimit;
    private final Duration visibilityTimeout;

    public QueuePollContext(Jdbi jdbi, String queueName, int messageLimit, Duration visibilityTimeout, FluxSink<Message> sink) {
        this.jdbi = jdbi;
        this.queueName = queueName;
        this.sink = sink;
        this.messageLimit = messageLimit;
        this.visibilityTimeout = visibilityTimeout;
    }

    public Jdbi jdbi() {
        return jdbi;
    }

    public String queueName() {
        return queueName;
    }

    public FluxSink<Message> sink() {
        return sink;
    }

    public int messageLimit() {
        return messageLimit;
    }

    public void decrementMessageLimit(int amount) {
        messageLimit -= amount;
    }

    public boolean shouldContinue() {
        return messageLimit > 0 && !sink().isCancelled();
    }

    public Duration visibilityTimeout() {
        return visibilityTimeout;
    }
}
