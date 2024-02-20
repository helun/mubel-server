package io.mubel.provider.inmemory.queue;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayQueueTest {

    @Test
    void doit() throws InterruptedException {
        var queue = new DelayQueue<>();
        queue.put(createDelayed("1 sec", Duration.ofSeconds(1)));
        queue.put(createDelayed("500 ms", Duration.ofMillis(500)));
        System.err.println(queue.poll(2, TimeUnit.SECONDS));
        System.err.println(queue.poll(2, TimeUnit.SECONDS));
    }

    static Delayed createDelayed(String value, Duration delay) {
        long startTime = System.currentTimeMillis() + delay.toMillis();
        return new Delayed() {
            @Override
            public long getDelay(TimeUnit unit) {
                long diff = startTime - System.currentTimeMillis();
                return unit.convert(diff, TimeUnit.MILLISECONDS);
            }

            @Override
            public int compareTo(Delayed o) {
                return Long.compare(delay.toMillis(), o.getDelay(TimeUnit.MILLISECONDS));
            }

            @Override
            public String toString() {
                return value;
            }
        };
    }

}
