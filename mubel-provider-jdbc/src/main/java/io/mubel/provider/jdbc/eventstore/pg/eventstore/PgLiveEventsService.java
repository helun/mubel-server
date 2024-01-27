package io.mubel.provider.jdbc.eventstore.pg.eventstore;

import io.mubel.api.grpc.EventData;
import io.mubel.provider.jdbc.eventstore.AbstractLiveEventsService;
import io.mubel.provider.jdbc.eventstore.JdbcEventStore;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;

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
            Scheduler scheduler
    ) {
        super(eventStore, scheduler);
        this.dataSource = dataSource;
        this.channelName = channelName;
    }

    protected void run(FluxSink<EventData> emitter) throws Exception {
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
                        dispatchNewEvents(emitter);
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
        } catch (Exception e) {
            LOG.error("Error while closing connection", e);
        }
    }
}
