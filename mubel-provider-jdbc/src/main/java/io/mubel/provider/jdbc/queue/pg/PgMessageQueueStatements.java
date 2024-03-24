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
                          queue_name TEXT NOT NULL,
                          type TEXT NOT NULL,
                          payload BYTEA NOT NULL,
                          delay_ms INTEGER NOT NULL DEFAULT 0,
                          locked BOOLEAN NOT NULL DEFAULT FALSE,
                          created_at TIMESTAMP  DEFAULT CURRENT_TIMESTAMP,
                          visible_at TIMESTAMP,
                          lock_expires_at TIMESTAMP
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
