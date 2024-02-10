package io.mubel.provider.jdbc.eventstore.mysql;

import io.mubel.provider.jdbc.eventstore.EventStoreStatements;

import java.util.List;
import java.util.Objects;

public class MysqlEventStoreStatements extends EventStoreStatements {

    private static final List<String> DDL_TPL = List.of(
            """
                            CREATE TABLE %1$s_request_log(
                                id BINARY(16) NOT NULL PRIMARY KEY,
                                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                            );
                    """,
            """             
                    CREATE TABLE %1$s_events(
                      id BINARY(16) NOT NULL PRIMARY KEY,
                      stream_id BINARY(16) NOT NULL,
                      version INTEGER UNSIGNED NOT NULL,
                      type VARCHAR(255) NOT NULL,
                      created_at BIGINT NOT NULL,
                      data MEDIUMBLOB,
                      meta_data MEDIUMBLOB,
                      CONSTRAINT %1$s_sid_ver UNIQUE (stream_id, version)
                    );
                    """,
            """     
                    CREATE TABLE %1$s_all_events_subscription (
                        seq_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        event_id BINARY(16) NOT NULL
                    );
                    """,
            """     
                    CREATE TRIGGER %1$s_insert_event_seq AFTER INSERT ON %1$s_events FOR EACH ROW BEGIN INSERT INTO %1$s_all_events_subscription(event_id) VALUES (NEW.id);END;
                    """);

    private static final String APPEND_SQL_TPL = """
            INSERT INTO %s_events(
              id,
              stream_id,
              version,
              type,
              created_at,
              data,
              meta_data
              ) VALUES (UUID_TO_BIN(?,1),UUID_TO_BIN(?,1),?,?,?,?,?)
            """;

    private static final String SELECT_EVENTS_TPL = """
            SELECT
              BIN_TO_UUID(id,1),
              BIN_TO_UUID(stream_id,1),
              e.version,
              e.type,
              e.created_at,
              e.data,
              e.meta_data
            """;

    private static final String GET_SQL_TPL = SELECT_EVENTS_TPL + """
            ,0 AS seq_id
            FROM %1$s_events e
            WHERE stream_id = UUID_TO_BIN(?,1)
            AND version >= ?
            ORDER BY version
            LIMIT ?
            """;

    private static final String GET_MAX_VERSION_SQL_TPL = SELECT_EVENTS_TPL + """
            ,0 AS seq_id
            FROM %1$s_events e
            WHERE stream_id = UUID_TO_BIN(?,1)
            AND version BETWEEN ? AND ?
            ORDER BY version
            LIMIT ?
            """;

    private static final String REPLAY_SQL_TPL = SELECT_EVENTS_TPL + """
            ,s.seq_id
            FROM %1$s_events e
            JOIN %1$s_all_events_subscription s ON s.event_id = e.id
            WHERE s.seq_id > ?
            ORDER BY s.seq_id
            """;

    private static final String PAGED_REPLAY_SQL_TPL = SELECT_EVENTS_TPL + """
            ,s.seq_id
            FROM %1$s_events e
            JOIN %1$s_all_events_subscription s ON s.event_id = e.id
            WHERE s.seq_id > ?
            ORDER BY s.seq_id
            LIMIT ?
            """;

    private static final List<String> TRUNCATE_SQL_TPL = List.of(
            "TRUNCATE TABLE %1$s_events",
            "TRUNCATE TABLE %1$s_all_events_subscription",
            "TRUNCATE TABLE %1$s_request_log"
    );

    private static final List<String> DROP_SQL = List.of(
            "DROP TABLE %1$s_events, %1$s_all_events_subscription, %1$s_request_log",
            "DROP TRIGGER %1$s_insert_event_seq"
    );

    private static final String LOG_REQUEST_SQL_TPL = """
            INSERT IGNORE INTO %s_request_log(id) VALUES (?)
            """;

    public MysqlEventStoreStatements(String eventStoreName) {
        super(
                eventStoreName,
                APPEND_SQL_TPL.formatted(eventStoreName),
                LOG_REQUEST_SQL_TPL.formatted(eventStoreName),
                GET_SQL_TPL.formatted(eventStoreName),
                GET_MAX_VERSION_SQL_TPL.formatted(eventStoreName),
                PAGED_REPLAY_SQL_TPL.formatted(eventStoreName),
                DDL_TPL.stream().map(s -> s.formatted(eventStoreName)).toList(),
                DROP_SQL.stream().map(s -> s.formatted(eventStoreName)).toList()
        );
    }

    @Override
    public List<String> truncate() {
        return TRUNCATE_SQL_TPL.stream().map(s -> s.formatted(eventStoreName())).toList();
    }

    public static boolean isVersionConflictError(String violatedConstraint) {
        return Objects.requireNonNull(violatedConstraint).contains("events_sid_ver");
    }

    @Override
    public String replaySql() {
        return REPLAY_SQL_TPL.formatted(eventStoreName());
    }

    @Override
    public String getSequenceNoSql() {
        return "SELECT max(seq_id) AS seq_id FROM %s_all_events_subscription".formatted(eventStoreName());
    }

    @Override
    public String summarySql() {
        return """
                SELECT
                  COUNT(id) AS event_count,
                  COUNT(DISTINCT stream_id) AS stream_count
                FROM %s_events
                """.formatted(eventStoreName());
    }
}
