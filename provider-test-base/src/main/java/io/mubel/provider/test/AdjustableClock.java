package io.mubel.provider.test;

import java.time.*;

public class AdjustableClock extends Clock {

    private Instant instant;

    public AdjustableClock() {
        this.instant = Instant.now();
    }

    public AdjustableClock(Instant instant) {
        this.instant = instant;
    }

    public void setInstant(Instant instant) {
        this.instant = instant;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant instant() {
        return instant;
    }

    public void tick(Duration duration) {
        instant = instant.plus(duration);
    }
}
