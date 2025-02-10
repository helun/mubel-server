/*
 * mubel-provider-inmemory - Multi Backend Event Log
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
