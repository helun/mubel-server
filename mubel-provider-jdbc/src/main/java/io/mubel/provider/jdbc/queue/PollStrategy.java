package io.mubel.provider.jdbc.queue;

@FunctionalInterface
public interface PollStrategy {
    void poll(QueuePollContext context);
}
