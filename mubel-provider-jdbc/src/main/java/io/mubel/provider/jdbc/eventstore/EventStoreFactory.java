package io.mubel.provider.jdbc.eventstore;

import io.mubel.provider.jdbc.configuration.JdbcProviderProperties;
import io.mubel.provider.jdbc.eventstore.mysql.MysqlEventStoreStatements;
import io.mubel.provider.jdbc.eventstore.pg.PgErrorMapper;
import io.mubel.provider.jdbc.eventstore.pg.PgEventStoreStatements;
import io.mubel.provider.jdbc.eventstore.pg.PgLiveEventsService;
import io.mubel.provider.jdbc.queue.BatchDeleteStrategy;
import io.mubel.provider.jdbc.queue.DefaultDeleteStrategy;
import io.mubel.provider.jdbc.queue.JdbcMessageQueueService;
import io.mubel.provider.jdbc.queue.SimpleWaitStrategy;
import io.mubel.provider.jdbc.queue.mysql.MysqlMessageQueueStatements;
import io.mubel.provider.jdbc.queue.mysql.MysqlPollStrategy;
import io.mubel.provider.jdbc.queue.pg.PgMessageQueueStatements;
import io.mubel.provider.jdbc.queue.pg.PgPollStrategy;
import io.mubel.provider.jdbc.support.JdbcDataSources;
import io.mubel.provider.jdbc.support.SqlStatements;
import io.mubel.provider.jdbc.support.mysql.MysqlJdbiFactory;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.support.IdGenerator;
import org.jdbi.v3.core.Jdbi;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.time.Duration;

public class EventStoreFactory {

    private final JdbcDataSources dataSources;
    private final JdbcProviderProperties properties;
    private final Scheduler scheduler;
    private final IdGenerator idGenerator;

    public EventStoreFactory(
            JdbcDataSources dataSources,
            JdbcProviderProperties properties,
            Scheduler scheduler,
            IdGenerator idGenerator
    ) {
        this.dataSources = dataSources;
        this.properties = properties;
        this.scheduler = scheduler;
        this.idGenerator = idGenerator;
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
        var jdbi = MysqlJdbiFactory.create(dataSource);
        var queueStatements = new MysqlMessageQueueStatements();

        var statements = new MysqlEventStoreStatements(request.esid());
        var eventStore = new JdbcEventStore(
                jdbi,
                statements,
                new PgErrorMapper()
        );

        var provisioner = new JdbcEventStoreProvisioner(
                dataSource,
                SqlStatements.of(statements.ddl(), queueStatements.ddl()),
                SqlStatements.of(statements.dropSql(), queueStatements.dropSql())
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

        var messageQueueService = new JdbcMessageQueueService.Builder()
                .jdbi(jdbi)
                .statements(queueStatements)
                .idGenerator(idGenerator)
                .waitStrategy(new SimpleWaitStrategy(Duration.ofSeconds(30)))
                .pollStrategy(new MysqlPollStrategy(queueStatements))
                .deleteStrategy(new BatchDeleteStrategy(queueStatements))
                .build();

        return new JdbcEventStoreContext(
                eventStore,
                provisioner,
                replayService,
                liveService,
                messageQueueService
        );
    }

    private JdbcEventStoreContext createPostgresEventStore(DataSource dataSource, ProvisionCommand command) {
        var jdbi = Jdbi.create(dataSource);
        var statements = new PgEventStoreStatements(command.esid());
        var queueStatements = new PgMessageQueueStatements();

        var eventStore = new JdbcEventStore(
                jdbi,
                statements,
                new PgErrorMapper()
        );
        var provisioner = new JdbcEventStoreProvisioner(
                dataSource,
                SqlStatements.of(statements.ddl(), queueStatements.ddl()),
                SqlStatements.of(statements.dropSql(), queueStatements.dropSql())
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

        var messageQueueService = new JdbcMessageQueueService.Builder()
                .jdbi(jdbi)
                .statements(queueStatements)
                .idGenerator(idGenerator)
                .waitStrategy(new SimpleWaitStrategy(Duration.ofSeconds(30)))
                .pollStrategy(new PgPollStrategy(queueStatements))
                .deleteStrategy(new DefaultDeleteStrategy(queueStatements))
                .build();

        return new JdbcEventStoreContext(
                eventStore,
                provisioner,
                replayService,
                liveService,
                messageQueueService
        );
    }

}
