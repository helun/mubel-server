package io.mubel.provider.jdbc.queue;

import io.mubel.server.spi.support.TimeBudget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class SimpleWaitStrategy implements WaitStrategy {

    private final long maxWaitTimeMs;

    public SimpleWaitStrategy(Duration maxWaitTime) {
        maxWaitTimeMs = maxWaitTime.toMillis();
    }

    private static final Logger LOG = LoggerFactory.getLogger(SimpleWaitStrategy.class);

    @Override
    public void wait(TimeBudget timeBudget) {
        long remainingTime = Math.max(timeBudget.remainingTimeMs() - 100, 1);
        long waitTime = Math.min(maxWaitTimeMs, remainingTime);
        try {
            LOG.trace("Waiting for {} ms", waitTime);
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
