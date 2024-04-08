package io.mubel.server.scheduling;

import org.springframework.stereotype.Component;

/**
 * A class that calculates if an event is due for publishing.
 */
@Component
public class PublishTimeCalculator {

    /**
     * The threshold in milliseconds for when an event is due for publishing.
     * The effect is that the event with publish time less than this value
     * will be loaded in to memory and ready for publishing.
     */
    private long dueThresholdMs = 60 * 1000;
    /**
     * The threshold in milliseconds for when an event can be published early.
     * The effect may be greater performance because more events may be published in a batch.
     */
    private long publishThresholdMs = 20;

    public boolean isDueForPublish(long publishTime) {
        return calculateDelay(publishTime) < dueThresholdMs;
    }

    public long calculateDelay(long publishTime) {
        return publishTime - System.currentTimeMillis();
    }

    public long getDueThresholdMs() {
        return dueThresholdMs;
    }

    public long setDueThresholdMs(long dueThresholdMs) {
        return this.dueThresholdMs = dueThresholdMs;
    }

    public long getPublishThresholdMs() {
        return publishThresholdMs;
    }

    public void setPublishThresholdMs(long publishThresholdMs) {
        this.publishThresholdMs = publishThresholdMs;
    }
}
