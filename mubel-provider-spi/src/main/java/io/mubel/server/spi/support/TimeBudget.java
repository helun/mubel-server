package io.mubel.server.spi.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class TimeBudget {

    private static final Logger LOG = LoggerFactory.getLogger(TimeBudget.class);

    private final long deadline;

    private long timeRemaining;

    public TimeBudget(Duration duration) {
        this.timeRemaining = duration.toMillis();
        deadline = System.currentTimeMillis() + timeRemaining;
    }

    public boolean hasTimeRemaining() {
        timeRemaining = deadline - System.currentTimeMillis();
        if (timeRemaining > 0) {
            LOG.trace("time remaining: {}ms", timeRemaining);
        } else {
            LOG.trace("time budget exceeded by {}ms", -timeRemaining);
        }
        return timeRemaining > 0;
    }

    public long remainingTimeMs() {
        return Math.max(timeRemaining, 0);
    }
}
