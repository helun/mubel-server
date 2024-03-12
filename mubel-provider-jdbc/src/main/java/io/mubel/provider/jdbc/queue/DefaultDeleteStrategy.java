package io.mubel.provider.jdbc.queue;

import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class DefaultDeleteStrategy implements DeleteStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDeleteStrategy.class);

    private final MessageQueueStatements statements;

    public DefaultDeleteStrategy(MessageQueueStatements statements) {
        this.statements = requireNonNull(statements);
    }

    @Override
    public void delete(Jdbi jdbi, Collection<UUID> uuids) {
        if (uuids.isEmpty()) {
            return;
        }
        jdbi.useTransaction(h -> {
            int rowCount = h.createUpdate(statements.delete())
                    .bindList("IDS", uuids)
                    .execute();
            LOG.debug("Deleted {} messages", rowCount);
        });
    }
}
