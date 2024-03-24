package io.mubel.provider.jdbc.queue;

import java.util.List;

public interface MessageQueueStatements {

    List<String> ddl();

    default List<String> dropSql() {
        return List.of("DROP TABLE IF EXISTS message_queue");
    }

    String insert();

    default String delete() {
        return "DELETE FROM message_queue WHERE message_id IN (<IDS>)";
    }

    default String enforceVisibilityTimeout() {
        return """
                UPDATE message_queue SET locked = false, lock_expires_at = NULL
                WHERE lock_expires_at <= CURRENT_TIMESTAMP
                """;
    }

}
