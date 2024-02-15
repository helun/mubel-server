package io.mubel.server.spi.support;

import java.time.Duration;

public class TimeBudget {

    private long timeRemaining;
    private long lastCheck;

    public TimeBudget(Duration duration) {
        this.timeRemaining = duration.toMillis();
        lastCheck = System.currentTimeMillis();
    }

    public boolean hasTimeRemaining() {
        timeRemaining = timeRemaining - (System.currentTimeMillis() - lastCheck);
        return timeRemaining > 0;
    }

    public long remainingTimeMs() {
        return timeRemaining;
    }
}
