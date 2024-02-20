package io.mubel.provider.jdbc.queue;

import io.mubel.server.spi.support.TimeBudget;

import java.time.Duration;

public class SimpleWaitStrategy implements WaitStrategy {

    private final long maxWaitTimeMs;

    public SimpleWaitStrategy(Duration maxWaitTime) {
        maxWaitTimeMs = maxWaitTime.toMillis();
    }

    @Override
    public void wait(TimeBudget timeBudget) {
        long remainingTime = timeBudget.remainingTimeMs();
        long waitTime = Math.min(maxWaitTimeMs, remainingTime);
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
