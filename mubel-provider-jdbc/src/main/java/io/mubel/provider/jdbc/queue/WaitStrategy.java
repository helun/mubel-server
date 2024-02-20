package io.mubel.provider.jdbc.queue;

import io.mubel.server.spi.support.TimeBudget;

public interface WaitStrategy {
    void wait(TimeBudget timeBudget);

}
