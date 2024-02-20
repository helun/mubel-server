package io.mubel.provider.jdbc.queue;

import java.time.Duration;
import java.util.List;

public class MessageQueueStatements {

    public List<String> ddl() {
        return List.of("""
                CREATE TABLE message_queue (
                          message_id UUID PRIMARY KEY,
                          queue_name VARCHAR(255) NOT NULL,
                          type VARCHAR(255) NOT NULL,
                          payload BYTEA NOT NULL,
                          delay_ms INTEGER NOT NULL DEFAULT 0,
                          locked BOOLEAN NOT NULL DEFAULT FALSE,
                          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                          visible_at TIMESTAMP WITH TIME ZONE NOT NULL,
                          lock_expires_at TIMESTAMP WITH TIME ZONE
                      );
                      
                CREATE INDEX idx_queue_name_visible_at ON message_queue(queue_name, visible_at);
                """);
    }

    public String insert() {
        return """
                INSERT INTO message_queue (message_id, queue_name, type, payload, delay_ms, visible_at)
                VALUES (?, ?, ?, ?, ?, ?)
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

    public String delete() {
        return "DELETE FROM message_queue WHERE message_id IN (<IDS>)";
    }

    public String visibilityTimeoutExpression(Duration timeout) {
        return timeout.toMillis() + " milliseconds'";
    }

    public String enforceVisibilityTimeout() {
        return """
                UPDATE message_queue SET locked = false, lock_expires_at = NULL
                WHERE lock_expires_at <= CURRENT_TIMESTAMP
                """;
    }
}
