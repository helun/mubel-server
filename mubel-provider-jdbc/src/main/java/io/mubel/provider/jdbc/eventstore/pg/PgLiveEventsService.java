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
package io.mubel.provider.jdbc.eventstore.pg;

import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.provider.jdbc.eventstore.JdbcEventStore;
import io.mubel.provider.jdbc.eventstore.JdbcLiveEventsService;
import org.postgresql.PGNotification;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Set;

public class PgLiveEventsService extends JdbcLiveEventsService {

    private static final Set<String> CLOSED_SQL_STATES = Set.of("08003", "08006");

    private static final Logger LOG = LoggerFactory.getLogger(PgLiveEventsService.class);
    private final String channelName;
    private final DataSource dataSource;

    public PgLiveEventsService(
            DataSource dataSource,
            String channelName,
            JdbcEventStore eventStore,
            Scheduler scheduler
    ) {
        super(eventStore, scheduler);
        this.dataSource = dataSource;
        this.channelName = channelName;
    }

    protected void run(FluxSink<EventData> emitter) throws Exception {
        try (var connection = dataSource.getConnection()) {
            PgConnection pgConnection = connection.unwrap(PgConnection.class);
            try (final var stmt = pgConnection.createStatement()) {
                LOG.info("Listening to channel {}", channelName);
                stmt.execute("LISTEN " + channelName);
                while (shouldRun(emitter, pgConnection)) {
                    var notifications = pgConnection.getNotifications(2000);
                    if (hasNotification(notifications)) {
                        LOG.debug("Received notifications: {}", notifications.length);
                        dispatchNewEvents(emitter);
                    }
                }
            }
        } catch (SQLException sqle) {
            if (CLOSED_SQL_STATES.contains(sqle.getSQLState())) {
                LOG.warn("connection closed");
            } else {
                throw sqle;
            }
        }
    }

    private static boolean hasNotification(PGNotification[] notifications) {
        return notifications != null && notifications.length > 0;
    }

    private boolean shouldRun(FluxSink<EventData> emitter, PgConnection pgConnection) throws SQLException {
        return shouldRun() && pgConnection.isValid(250) && !emitter.isCancelled();
    }

    @Override
    protected void onStop() {
        LOG.debug("stopping listener for channel {}", channelName);
    }
}
