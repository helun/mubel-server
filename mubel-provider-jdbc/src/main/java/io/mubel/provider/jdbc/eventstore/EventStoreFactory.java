package io.mubel.provider.jdbc.eventstore;

import io.mubel.provider.jdbc.configuration.JdbcProviderProperties;
import io.mubel.provider.jdbc.eventstore.mysql.MysqlEventStoreStatements;
import io.mubel.provider.jdbc.eventstore.pg.PgErrorMapper;
import io.mubel.provider.jdbc.eventstore.pg.PgEventStoreStatements;
import io.mubel.provider.jdbc.eventstore.pg.PgLiveEventsService;
import io.mubel.provider.jdbc.support.JdbcDataSources;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.model.ProvisionCommand;
import org.jdbi.v3.core.Jdbi;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;

public class EventStoreFactory {

    private final JdbcDataSources dataSources;
    private final JdbcProviderProperties properties;
    private final Scheduler scheduler;

    public EventStoreFactory(JdbcDataSources dataSources, JdbcProviderProperties properties, Scheduler scheduler) {
        this.dataSources = dataSources;
        this.properties = properties;
        this.scheduler = scheduler;
    }

    public JdbcEventStoreContext create(ProvisionCommand command) {
        var backendProps = resolveBackendProperties(command);
        var dataSource = dataSources.get(backendProps.getDataSource());
        if (dataSource == null) {
            throw new IllegalArgumentException("No datasource found for backend: " + command.storageBackendName());
        }

        return switch (dataSource.backendType()) {
            case PG -> createPostgresEventStore(dataSource.dataSource(), command);
            case MYSQL -> createMysqlEventStore(dataSource.dataSource(), command);
            default -> throw new IllegalArgumentException("No event store found for backend: " + backendProps);
        };
    }

    private JdbcProviderProperties.BackendProperties resolveBackendProperties(ProvisionCommand request) {
        var backendName = request.storageBackendName();
        return properties.findBackend(backendName)
                .orElseThrow(() -> new ResourceNotFoundException("No backend found for name: " + backendName));
    }

    private JdbcEventStoreContext createMysqlEventStore(
            DataSource dataSource,
            ProvisionCommand request
    ) {
        var jdbi = Jdbi.create(dataSource);
        var statements = new MysqlEventStoreStatements(request.esid());
        var eventStore = new JdbcEventStore(
                jdbi,
                statements,
                new PgErrorMapper()
        );
        var provisioner = new JdbcEventStoreProvisioner(
                dataSource,
                statements
        );
        var replayService = new JdbcReplayService(
                jdbi,
                statements,
                scheduler
        );
        var liveService = new PollingLiveEventsService(
                1000,
                eventStore,
                scheduler
        );
        return new JdbcEventStoreContext(
                eventStore,
                provisioner,
                replayService,
                liveService
        );
    }

    private JdbcEventStoreContext createPostgresEventStore(DataSource dataSource, ProvisionCommand command) {
        var jdbi = Jdbi.create(dataSource);
        PgEventStoreStatements statements = new PgEventStoreStatements(command.esid());
        var eventStore = new JdbcEventStore(
                jdbi,
                statements,
                new PgErrorMapper()
        );
        var provisioner = new JdbcEventStoreProvisioner(
                dataSource,
                statements
        );
        var replayService = new JdbcReplayService(
                jdbi,
                statements,
                scheduler
        );
        var liveService = new PgLiveEventsService(
                dataSource,
                command.esid() + "_live",
                eventStore,
                scheduler
        );
        return new JdbcEventStoreContext(
                eventStore,
                provisioner,
                replayService,
                liveService
        );
    }

}
