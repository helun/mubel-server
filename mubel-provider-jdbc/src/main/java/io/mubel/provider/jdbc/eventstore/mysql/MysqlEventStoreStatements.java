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
package io.mubel.provider.jdbc.eventstore.mysql;

import io.mubel.provider.jdbc.eventstore.EventStoreStatements;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
                      revision INTEGER UNSIGNED NOT NULL,
                      type VARCHAR(255) NOT NULL,
                      created_at BIGINT NOT NULL,
                      data MEDIUMBLOB,
                      meta_data MEDIUMBLOB,
                      CONSTRAINT %1$s_sid_ver UNIQUE (stream_id, revision)
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
              revision,
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
              e.revision,
              e.type,
              e.created_at,
              e.data,
              e.meta_data
            """;

    private static final String GET_SQL_TPL = SELECT_EVENTS_TPL + """
            ,0 AS seq_id
            FROM %1$s_events e
            WHERE stream_id = UUID_TO_BIN(?,1)
            AND revision >= ?
            ORDER BY revision
            LIMIT ?
            """;

    private static final String GET_MAX_REVISION_SQL_TPL = SELECT_EVENTS_TPL + """
            ,0 AS seq_id
            FROM %1$s_events e
            WHERE stream_id = UUID_TO_BIN(?,1)
            AND revision BETWEEN ? AND ?
            ORDER BY revision
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
            "DROP TABLE %1$s_events, %1$s_all_events_subscription, %1$s_request_log"
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
                GET_MAX_REVISION_SQL_TPL.formatted(eventStoreName),
                PAGED_REPLAY_SQL_TPL.formatted(eventStoreName),
                DDL_TPL.stream().map(s -> s.formatted(eventStoreName)).toList(),
                DROP_SQL.stream().map(s -> s.formatted(eventStoreName)).toList()
        );
    }

    @Override
    public List<String> truncate() {
        return TRUNCATE_SQL_TPL.stream().map(s -> s.formatted(eventStoreName())).toList();
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
        return "SELECT COALESCE(max(seq_id), 0) AS seq_id FROM %s_all_events_subscription".formatted(eventStoreName());
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

    @Override
    public String currentRevisionsSql(int paramSize) {
        var params = IntStream.range(0, paramSize)
                .mapToObj(i -> "UUID_TO_BIN(?,1)")
                .collect(Collectors.joining(","));
        return """
                SELECT BIN_TO_UUID(stream_id, 1), MAX(revision) AS max_revision
                FROM %s_events
                WHERE stream_id IN (%s)
                GROUP BY stream_id;
                """.formatted(eventStoreName(), params);
    }

}
