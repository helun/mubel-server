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

import io.mubel.provider.jdbc.queue.MessageQueueStatements;

import java.util.List;

public class PgMessageQueueStatements implements MessageQueueStatements {

    @Override
    public List<String> ddl() {
        //
        return List.of("""
                CREATE TABLE IF NOT EXISTS message_queue (
                          message_id UUID PRIMARY KEY,
                          created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
                          visible_at TIMESTAMP,
                          lock_expires_at TIMESTAMP,
                          delay_ms INTEGER NOT NULL DEFAULT 0,
                          locked BOOLEAN NOT NULL DEFAULT FALSE,
                          queue_name TEXT NOT NULL,
                          type TEXT NOT NULL,
                          payload BYTEA NOT NULL
                      );
                CREATE INDEX IF NOT EXISTS idx_queue_name_visible_at ON message_queue(queue_name, visible_at);
                """);
    }

    @Override
    public String insert() {
        return """
                INSERT INTO message_queue (message_id, queue_name, type, payload, delay_ms, visible_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP + (?::int * INTERVAL '1 millisecond'))
                """;
    }

    public String poll() {
        return """
                WITH selected_messages AS (
                    SELECT message_id
                    FROM message_queue
                    WHERE queue_name = ?
                    AND locked = false
                    AND visible_at <= CURRENT_TIMESTAMP
                    ORDER BY visible_at ASC
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE message_queue
                SET locked = true, lock_expires_at = ?
                FROM selected_messages
                WHERE message_queue.message_id = selected_messages.message_id
                RETURNING message_queue.message_id, queue_name, type, payload;
                """;
    }
}
