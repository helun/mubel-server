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

import io.mubel.provider.jdbc.queue.MessageQueueStatements;

import java.util.List;

public class MysqlMessageQueueStatements implements MessageQueueStatements {

    @Override
    public List<String> ddl() {
        return List.of("""
                CREATE TABLE IF NOT EXISTS message_queue (
                    message_id BINARY(16) NOT NULL PRIMARY KEY,
                    lock_expires_at TIMESTAMP(3) NULL,
                    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    visible_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    delay_ms INTEGER NOT NULL DEFAULT 0,
                    queue_name VARCHAR(255) NOT NULL,
                    locked BOOLEAN NOT NULL DEFAULT FALSE,
                    type VARCHAR(255) NOT NULL,
                    payload MEDIUMBLOB NOT NULL,
                    INDEX idx_queue_name_visible_at (queue_name, visible_at)
                )
                """
        );
    }

    @Override
    public String insert() {
        return """
                INSERT INTO message_queue (message_id, queue_name, type, payload, delay_ms, visible_at)
                VALUES (uuid_to_bin(?, 1), ?, ?, ?, ?, CURRENT_TIMESTAMP(3) + INTERVAL (? * 1000) MICROSECOND)
                """;
    }

    public String poll() {
        return """
                SELECT
                    bin_to_uuid(message_id, 1) as message_id,
                    queue_name,
                    type,
                    payload,
                    message_id AS raw_message_id
                FROM message_queue
                WHERE locked = FALSE
                AND queue_name = ?
                AND visible_at <= CURRENT_TIMESTAMP(3)
                ORDER BY visible_at ASC
                LIMIT ? FOR UPDATE
                """;
    }

    public String lock() {
        return """
                UPDATE message_queue
                SET locked = TRUE, lock_expires_at = :expires_at
                WHERE message_id IN (<IDS>);
                """;
    }

    @Override
    public String delete() {
        return "DELETE FROM message_queue WHERE message_id = uuid_to_bin(?, 1)";
    }
}
