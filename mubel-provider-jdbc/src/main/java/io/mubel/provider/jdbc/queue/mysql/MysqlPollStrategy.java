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
package io.mubel.provider.jdbc.queue.mysql;

import com.fasterxml.uuid.impl.UUIDUtil;
import io.mubel.provider.jdbc.queue.PollStrategy;
import io.mubel.provider.jdbc.queue.QueuePollContext;
import io.mubel.server.spi.queue.Message;
import org.jdbi.v3.core.Handle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class MysqlPollStrategy implements PollStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlPollStrategy.class);

    private final MysqlMessageQueueStatements statements;

    public MysqlPollStrategy(MysqlMessageQueueStatements statements) {
        this.statements = statements;
    }

    @Override
    public void poll(QueuePollContext context) {
        final var polled = doPoll(context);
        if (!polled.isEmpty()) {
            final var sink = context.sink();
            polled.forEach(sink::next);
            context.decrementMessageLimit(polled.size());
        }
    }

    private List<Message> doPoll(QueuePollContext context) {
        return context.jdbi().inTransaction(h -> {
            final var rawIds = new ArrayList<byte[]>(context.messageLimit());
            LOG.trace("Polling with message limit {}", context.messageLimit());
            final var messages = h.createQuery(statements.poll())
                    .bind(0, context.queueName())
                    .bind(1, context.messageLimit())
                    .map(view -> {
                        rawIds.add(view.getColumn(5, byte[].class));
                        return new Message(
                                UUIDUtil.uuid(view.getColumn(1, String.class)),
                                view.getColumn(2, String.class),
                                view.getColumn(3, String.class),
                                view.getColumn(4, byte[].class)
                        );
                    }).list();
            if (LOG.isDebugEnabled()) {
                dumpQueue(h, context.queueName());
            }
            LOG.trace("Polled {} messages", messages.size());
            if (!messages.isEmpty()) {
                lockRows(context, h, rawIds);
            }
            return messages;
        });
    }

    private void dumpQueue(Handle h, String queueName) {
        h.createQuery("SELECT * FROM message_queue WHERE queue_name = ? ORDER BY created_at DESC LIMIT 10")
                .bind(0, queueName)
                .mapToMap()
                .list()
                .forEach(row -> LOG.trace("{}", row));
    }

    private void lockRows(QueuePollContext context, Handle h, List<byte[]> rawIds) {
        final var expiresAt = Instant.now().plus(context.visibilityTimeout());
        var lockedRows = h.createUpdate(statements.lock())
                .bind("expires_at", expiresAt)
                .bindList("IDS", rawIds)
                .execute();
        LOG.debug("Locked {} messages", lockedRows);
    }
}
