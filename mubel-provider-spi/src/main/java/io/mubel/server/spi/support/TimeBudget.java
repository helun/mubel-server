/*
 * mubel-provider-spi - Multi Backend Event Log
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
