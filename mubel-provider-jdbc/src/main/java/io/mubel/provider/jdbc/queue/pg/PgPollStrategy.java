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
package io.mubel.provider.jdbc.queue.pg;

import io.mubel.provider.jdbc.queue.PollStrategy;
import io.mubel.provider.jdbc.queue.QueuePollContext;
import io.mubel.server.spi.queue.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;

public class PgPollStrategy implements PollStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PgPollStrategy.class);

    private final PgMessageQueueStatements statements;

    public PgPollStrategy(PgMessageQueueStatements statements) {
        this.statements = statements;
    }

    @Override
    public void poll(QueuePollContext context) {
        var expiresAt = Instant.now().plus(context.visibilityTimeout());
        int polledMessageCount = doPoll(context, expiresAt);
        context.decrementMessageLimit(polledMessageCount);
    }

    private int doPoll(QueuePollContext context, Instant expiresAt) {
        LOG.trace("Polling with message limit {}", context.messageLimit());
        return context.jdbi().withHandle(h -> h.createQuery(statements.poll())
                .bind(0, context.queueName())
                .bind(1, context.messageLimit())
                .bind(2, expiresAt)
                .map(view -> new Message(
                        view.getColumn(1, UUID.class),
                        view.getColumn(2, String.class),
                        view.getColumn(3, String.class),
                        view.getColumn(4, byte[].class)
                ))
                .stream()
                .map(msg -> {
                    context.sink().next(msg);
                    return 1;
                }).count()
        ).intValue();
    }
}
