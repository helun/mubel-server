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
