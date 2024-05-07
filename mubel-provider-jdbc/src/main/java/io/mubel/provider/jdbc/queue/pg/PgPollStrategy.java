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
        LOG.debug("Polling with message limit {}", context.messageLimit());
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
