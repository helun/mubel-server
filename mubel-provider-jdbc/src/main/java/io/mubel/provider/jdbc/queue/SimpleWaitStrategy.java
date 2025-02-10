/*
 * mubel-provider-jdbc - mubel-provider-jdbc
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
