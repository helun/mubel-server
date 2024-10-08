package io.mubel.provider.jdbc.eventstore.pg;

import com.fasterxml.uuid.impl.UUIDUtil;
import io.mubel.provider.jdbc.eventstore.EventStoreStatements;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
              created_at BIGINT NOT NULL,
              revision INTEGER NOT NULL,
              type TEXT NOT NULL,
              data BYTEA,
              meta_data BYTEA
            );
            
            CREATE UNIQUE INDEX events_sid_ver ON %1$s.events(stream_id, revision);
            
            CREATE TABLE %1$s.all_events_subscription (
                seq_id bigint GENERATED ALWAYS AS IDENTITY,
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
              revision,
              type,
              created_at,
              data,
              meta_data
              ) VALUES (?,?,?,?,?,?,?)
            """;

    private static final String SELECT_EVENTS_TPL = """
            SELECT
              e.id,
              e.stream_id,
              e.revision,
              e.type,
              e.created_at,
              e.data,
              e.meta_data
            """;

    private static final String GET_SQL_TPL = SELECT_EVENTS_TPL + """
            ,0 AS seq_id
            FROM %1$s.events e
            WHERE stream_id = ?
            AND revision >= ?
            ORDER BY revision
            LIMIT ?
            """;

    private static final String GET_MAX_REVISION_SQL_TPL = SELECT_EVENTS_TPL + """
            ,0 AS seq_id
            FROM %1$s.events e
            WHERE stream_id = ?
            AND revision BETWEEN ? AND ?
            ORDER BY revision
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
                GET_MAX_REVISION_SQL_TPL.formatted(eventStoreName),
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

    public static boolean isRevisionConflictError(String violatedConstraint) {
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
    public String summarySql() {
        return """
                SELECT
                  COUNT(id) AS event_count,
                  COUNT(DISTINCT stream_id) AS stream_count
                FROM %s.events
                """.formatted(eventStoreName());
    }

    @Override
    public Object convertUUID(String value) {
        return UUIDUtil.uuid(value);
    }

    @Override
    public Iterable<?> convertUUIDs(Collection<String> input) {
        var result = new ArrayList<UUID>(input.size());
        for (var id : input) {
            result.add(UUIDUtil.uuid(id));
        }
        return result;
    }

    @Override
    public String currentRevisionsSql(int paramSize) {
        var params = IntStream.range(0, paramSize)
                .mapToObj(i -> "?")
                .collect(Collectors.joining(","));
        return """
                SELECT stream_id, MAX(revision) AS max_revision
                FROM %s.events
                WHERE stream_id IN (%s)
                GROUP BY stream_id;
                """.formatted(
                eventStoreName(),
                params
        );
    }
}
