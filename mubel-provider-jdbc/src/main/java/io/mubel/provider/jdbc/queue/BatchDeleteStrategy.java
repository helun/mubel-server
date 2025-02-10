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
