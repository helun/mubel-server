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

import io.mubel.server.spi.queue.Message;
import org.jdbi.v3.core.Jdbi;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueuePollContext {

    private final Jdbi jdbi;
    private final String queueName;

    private final FluxSink<Message> sink;
    private final AtomicBoolean shouldRun;
    private int messageLimit;
    private final Duration visibilityTimeout;

    public QueuePollContext(Jdbi jdbi,
                            String queueName,
                            int messageLimit,
                            Duration visibilityTimeout,
                            FluxSink<Message> sink,
                            AtomicBoolean shouldRun
    ) {
        this.jdbi = jdbi;
        this.queueName = queueName;
        this.sink = sink;
        this.messageLimit = messageLimit;
        this.visibilityTimeout = visibilityTimeout;
        this.shouldRun = shouldRun;
    }

    public Jdbi jdbi() {
        return jdbi;
    }

    public String queueName() {
        return queueName;
    }

    public FluxSink<Message> sink() {
        return sink;
    }

    public int messageLimit() {
        return messageLimit;
    }

    public void decrementMessageLimit(int amount) {
        messageLimit -= amount;
    }

    public boolean shouldContinue() {
        return messageLimit > 0 && !sink().isCancelled() && shouldRun.get();
    }

    public Duration visibilityTimeout() {
        return visibilityTimeout;
    }
}
