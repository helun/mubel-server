package io.mubel.provider.jdbc.eventstore.pg;

import io.mubel.provider.jdbc.eventstore.EventStoreStatements;

import java.util.Objects;

public class PgEventStoreStatements extends EventStoreStatements {

    private static final String DDL_TPL = """
            CREATE SCHEMA %1$s;
                        
            CREATE TABLE %1$s.request_log (
                id UUID PRIMARY KEY,
                created_at timestamp NOT NULL DEFAULT now()
              );
                        
            CREATE TABLE %1$s.events (
              id UUID PRIMARY KEY,
              stream_id UUID NOT NULL,
              version INTEGER NOT NULL,
              type TEXT NOT NULL,
              created_at bigint NOT NULL,
              data BYTEA,
              meta_data BYTEA,
              seq_no BIGINT
            );
                        
            CREATE UNIQUE INDEX events_sid_ver ON %1$s.events(stream_id, version);
            CREATE UNIQUE INDEX events_seq_no ON %1$s.events(seq_no);
              """;

    private static final String APPEND_SQL_TPL = """
            INSERT INTO %s.events(
              id,
              stream_id,
              version,
              type,
              created_at,
              data,
              meta_data,
              seq_no) VALUES (?,?,?,?,?,?,?,?)
            """;

    private static final String INSERT_EVENT_TYPE_SQL_TPL = """
            INSERT INTO %s.event_types(type) VALUES (?)
            ON CONFLICT(type) DO NOTHING
            """;

    private static final String SELECT_ALL_EVENT_TYPES_SQL_TPL = """
            SELECT id, type FROM %s.event_types
            """;

    private static final String INSERT_STREAM_SQL_TPL = """
            INSERT INTO %s.streams(stream_id) VALUES (?)
            """;

    private static final String SELECT_STREAM_IDS_SQL_TPL = """
            SELECT stream_id, id FROM %s.streams WHERE stream_id IN (<streamIds>)
            """;
    private static final String SELECT_EVENTS_TPL = """
            SELECT
              e.id,
              e.stream_id,
              e.version,
              e.type,
              e.created_at,
              e.seq_no,
              e.data,
              e.meta_data
            FROM %1$s.events e
            """;

    private static final String GET_SQL_TPL = SELECT_EVENTS_TPL + """
            WHERE stream_id = ?
            AND version >= ?
            ORDER BY version
            LIMIT ?
            """;

    private static final String GET_MAX_VERSION_SQL_TPL = SELECT_EVENTS_TPL + """
            WHERE stream_id = ?
            AND version BETWEEN ? AND ?
            ORDER BY version
            LIMIT ?
            """;

    private static final String REPLAY_SQL_TPL = SELECT_EVENTS_TPL + """
            WHERE seq_no > ?
            ORDER BY seq_no
            """;

    private static final String PAGED_REPLAY_SQL_TPL = SELECT_EVENTS_TPL + """
            WHERE seq_no > ?
            ORDER BY seq_no
            LIMIT ?
            """;

    private static final String TRUNCATE_SQL_TPL = """
            TRUNCATE table %1$s.events;
            """;

    private static final String DROP_SQL = """
            DROP SCHEMA IF EXISTS %1$s CASCADE;
            """;

    private static final String LOG_REQUEST_SQL_TPL = """
            INSERT INTO %s.request_log(id) VALUES (?) ON CONFLICT DO NOTHING
            """;

    public PgEventStoreStatements(String eventStoreName) {
        super(
                eventStoreName,
                APPEND_SQL_TPL.formatted(eventStoreName),
                LOG_REQUEST_SQL_TPL.formatted(eventStoreName),
                INSERT_EVENT_TYPE_SQL_TPL.formatted(eventStoreName),
                INSERT_STREAM_SQL_TPL.formatted(eventStoreName),
                SELECT_STREAM_IDS_SQL_TPL.formatted(eventStoreName),
                GET_SQL_TPL.formatted(eventStoreName),
                GET_MAX_VERSION_SQL_TPL.formatted(eventStoreName),
                PAGED_REPLAY_SQL_TPL.formatted(eventStoreName),
                SELECT_ALL_EVENT_TYPES_SQL_TPL.formatted(eventStoreName),
                DDL_TPL.formatted(eventStoreName),
                DROP_SQL.formatted(eventStoreName)
        );
    }

    @Override
    public String truncate() {
        return TRUNCATE_SQL_TPL.formatted(eventStoreName());
    }

    public static boolean isVersionConflictError(String violatedConstraint) {
        return Objects.requireNonNull(violatedConstraint).contains("events_sid_ver");
    }
}
