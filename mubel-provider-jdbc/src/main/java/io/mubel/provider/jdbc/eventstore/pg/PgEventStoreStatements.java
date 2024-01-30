package io.mubel.provider.jdbc.eventstore.pg;

import io.mubel.provider.jdbc.eventstore.EventStoreStatements;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

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
              meta_data BYTEA
            );
                        
            CREATE UNIQUE INDEX events_sid_ver ON %1$s.events(stream_id, version);
                        
            CREATE TABLE %1$s.all_events_subscription (
                seq_id SERIAL8 PRIMARY KEY,
                event_id UUID NOT NULL
            );
                        
            CREATE OR REPLACE FUNCTION %1$s_insert_event_seq() RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO %1$s.all_events_subscription(event_id) VALUES (NEW.id);
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
                        
            CREATE OR REPLACE FUNCTION %1$s_notify_live() RETURNS TRIGGER AS $$
            DECLARE
                payload TEXT;
            BEGIN
                PERFORM pg_notify('%1$s_live', 'a');
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
                        
            CREATE TRIGGER %1$s_trigger_after_insert_event
            AFTER INSERT ON %1$s.events
            FOR EACH ROW EXECUTE FUNCTION %1$s_insert_event_seq();
                        
            CREATE TRIGGER %1$s_trigger_after_insert_all_sub
            AFTER INSERT ON %1$s.all_events_subscription
            FOR EACH ROW EXECUTE FUNCTION %1$s_notify_live();
            """;

    private static final String APPEND_SQL_TPL = """
            INSERT INTO %s.events(
              id,
              stream_id,
              version,
              type,
              created_at,
              data,
              meta_data
              ) VALUES (?,?,?,?,?,?,?)
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
              e.data,
              e.meta_data
            """;

    private static final String GET_SQL_TPL = SELECT_EVENTS_TPL + """
            ,0 AS seq_id
            FROM %1$s.events e
            WHERE stream_id = ?
            AND version >= ?
            ORDER BY version
            LIMIT ?
            """;

    private static final String GET_MAX_VERSION_SQL_TPL = SELECT_EVENTS_TPL + """
            ,0 AS seq_id
            FROM %1$s.events e
            WHERE stream_id = ?
            AND version BETWEEN ? AND ?
            ORDER BY version
            LIMIT ?
            """;

    private static final String REPLAY_SQL_TPL = SELECT_EVENTS_TPL + """
            ,s.seq_id
            FROM %1$s.events e
            JOIN %1$s.all_events_subscription s ON s.event_id = e.id
            WHERE s.seq_id > ?
            ORDER BY s.seq_id
            """;

    private static final String PAGED_REPLAY_SQL_TPL = SELECT_EVENTS_TPL + """
            ,s.seq_id
            FROM %1$s.events e
            JOIN %1$s.all_events_subscription s ON s.event_id = e.id
            WHERE s.seq_id > ?
            ORDER BY s.seq_id
            LIMIT ?
            """;

    private static final String TRUNCATE_SQL_TPL = """
            TRUNCATE table %1$s.events, %1$s.all_events_subscription RESTART IDENTITY CASCADE;
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
                GET_SQL_TPL.formatted(eventStoreName),
                GET_MAX_VERSION_SQL_TPL.formatted(eventStoreName),
                PAGED_REPLAY_SQL_TPL.formatted(eventStoreName),
                List.of(DDL_TPL.formatted(eventStoreName)),
                List.of(DROP_SQL.formatted(eventStoreName))
        );
    }

    public static String liveChannelName(String eventStoreName) {
        return eventStoreName + "_live";
    }

    @Override
    public List<String> truncate() {
        return List.of(TRUNCATE_SQL_TPL.formatted(eventStoreName()));
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
        return "SELECT COALESCE(max(seq_id), 0) AS seq_id FROM %s.all_events_subscription".formatted(eventStoreName());
    }

    @Override
    public Object convertUUID(String value) {
        return UUID.fromString(value);
    }
}
