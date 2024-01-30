package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.provider.jdbc.eventstore.configuration.JdbcProviderProperties;
import io.mubel.provider.jdbc.eventstore.mysql.MysqlEventStoreStatements;
import io.mubel.provider.jdbc.eventstore.pg.PgErrorMapper;
import io.mubel.provider.jdbc.eventstore.pg.PgEventStoreStatements;
import io.mubel.provider.jdbc.eventstore.pg.PgLiveEventsService;
import io.mubel.provider.jdbc.support.JdbcDataSources;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import org.jdbi.v3.core.Jdbi;
import org.springframework.stereotype.Service;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;

@Service
public class EventStoreFactory {

    private final JdbcDataSources dataSources;
    private final JdbcProviderProperties properties;
    private final Scheduler scheduler;

    public EventStoreFactory(JdbcDataSources dataSources, JdbcProviderProperties properties, Scheduler scheduler) {
        this.dataSources = dataSources;
        this.properties = properties;
        this.scheduler = scheduler;
    }

    public JdbcEventStoreContext create(ProvisionEventStoreRequest request) {
        var backendProps = resolveBackendProperties(request);
        var dataSource = dataSources.get(backendProps.getDataSource());
        if (dataSource == null) {
            throw new IllegalArgumentException("No datasource found for backend: " + request.getStorageBackendName());
        }

        return switch (dataSource.backendType()) {
            case PG -> createPostgresEventStore(dataSource.dataSource(), request);
            case MYSQL -> createMysqlEventStore(dataSource.dataSource(), request);
            default -> throw new IllegalArgumentException("No event store found for backend: " + backendProps);
        };
    }

    private JdbcProviderProperties.BackendProperties resolveBackendProperties(ProvisionEventStoreRequest request) {
        var backendName = request.getStorageBackendName();
        return properties.getBackends().stream()
                .filter(backendProperties -> backendProperties.getName().equals(backendName))
                .findFirst()
                .orElseThrow(() -> new ResourceNotFoundException("No backend found for name: " + backendName));
    }

    private JdbcEventStoreContext createMysqlEventStore(
            DataSource dataSource,
            ProvisionEventStoreRequest request
    ) {
        var jdbi = Jdbi.create(dataSource);
        var statements = new MysqlEventStoreStatements(request.getEsid());
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
                request.getEsid() + "_live",
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

    private JdbcEventStoreContext createPostgresEventStore(DataSource dataSource, ProvisionEventStoreRequest request) {
        var jdbi = Jdbi.create(dataSource);
        PgEventStoreStatements statements = new PgEventStoreStatements(request.getEsid());
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
                request.getEsid() + "_live",
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
