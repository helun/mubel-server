package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.provider.jdbc.eventstore.configuration.JdbcProviderProperties;
import io.mubel.provider.jdbc.eventstore.pg.PgErrorMapper;
import io.mubel.provider.jdbc.eventstore.pg.eventstore.PgEventStoreStatements;
import io.mubel.provider.jdbc.eventstore.pg.eventstore.PgLiveEventsService;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import org.jdbi.v3.core.Jdbi;
import org.springframework.stereotype.Service;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.util.Map;

@Service
public class EventStoreFactory {

    private final Map<String, DataSource> dataSources;
    private final JdbcProviderProperties properties;
    private final Scheduler scheduler;

    public EventStoreFactory(Map<String, DataSource> dataSources, JdbcProviderProperties properties, Scheduler scheduler) {
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

        var config = properties.findDataSource(backendProps.getDataSource())
                .orElseThrow();

        return switch (resolveStorageBackend(config)) {
            case POSTGRES -> createPostgresEventStore(dataSource, request);
            case MYSQL -> createMysqlEventStore(dataSource, request);
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
        throw new UnsupportedOperationException("MySQL is not supported yet");
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

    private StorageBackend resolveStorageBackend(JdbcProviderProperties.DataSourceProperties dsProps) {
        var url = dsProps.getUrl();
        if (url.startsWith("jdbc:postgresql")) {
            return StorageBackend.POSTGRES;
        } else if (url.startsWith("jdbc:mysql")) {
            return StorageBackend.MYSQL;
        } else {
            return StorageBackend.OTHER;
        }
    }

    enum StorageBackend {
        POSTGRES,
        MYSQL,
        OTHER
    }

}
