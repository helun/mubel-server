package io.mubel.provider.jdbc.queue;

import org.jdbi.v3.core.Jdbi;

import java.util.Collection;
import java.util.UUID;

public class BatchDeleteStrategy implements DeleteStrategy {

    private final MessageQueueStatements statements;

    public BatchDeleteStrategy(MessageQueueStatements statements) {
        this.statements = statements;
    }

    @Override
    public void delete(Jdbi jdbi, Collection<UUID> uuids) {
        if (uuids.isEmpty()) {
            // do nothing
        } else if (uuids.size() > 1) {
            batchDelete(jdbi, uuids);
        } else {
            deleteSingle(jdbi, uuids.iterator().next());
        }
    }

    private void deleteSingle(Jdbi jdbi, UUID id) {
        jdbi.useTransaction(h -> h.createUpdate(statements.delete())
                .bind(0, id)
                .execute());
    }

    private void batchDelete(Jdbi jdbi, Collection<UUID> uuids) {
        jdbi.useTransaction(h -> {
            var batch = h.prepareBatch(statements.delete());
            for (var uuid : uuids) {
                batch.bind(0, uuid);
                batch.add();
            }
            batch.execute();
        });
    }
}
