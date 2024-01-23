package io.mubel.provider.jdbc.eventstore.pg;

import io.mubel.provider.jdbc.eventstore.AbstractLiveEventsService;
import io.mubel.provider.jdbc.eventstore.JdbcEventStore;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.concurrent.Executor;

public class PgLiveEventsService extends AbstractLiveEventsService {

    private static final Logger LOG = LoggerFactory.getLogger(PgLiveEventsService.class);
    private final String channelName;
    private final DataSource dataSource;
    private PgConnection pgConnection;
    private java.sql.Connection connection;

    public PgLiveEventsService(
            DataSource dataSource,
            String channelName,
            JdbcEventStore eventStore,
            Executor executor
    ) {
        super(eventStore, executor);
        this.dataSource = dataSource;
        this.channelName = channelName;
    }

    protected void run() throws Exception {
        try (var connection = dataSource.getConnection()) {
            this.connection = connection;
            this.pgConnection = connection.unwrap(PgConnection.class);
            try (final var stmt = pgConnection.createStatement()) {
                LOG.info("Listening to channel {}", channelName);
                stmt.execute("LISTEN " + channelName);
                while (shouldRun()) {
                    var notifications = pgConnection.getNotifications(2000);
                    if (notifications != null && notifications.length > 0) {
                        LOG.debug("Received notification: {}", notifications);
                        dispatchNewEvents();
                    }
                }
            }
        }
    }

    @Override
    protected void onStop() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
            /*
            if (pgConnection != null && !pgConnection.isClosed()) {
                pgConnection.abort(executor);
            }
             */
        } catch (Exception e) {
            LOG.error("Error while closing connection", e);
        }
    }
}
