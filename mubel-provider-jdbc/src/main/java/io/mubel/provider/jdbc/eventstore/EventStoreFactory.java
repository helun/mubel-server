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
package io.mubel.provider.jdbc.eventstore;

import io.mubel.provider.jdbc.configuration.JdbcProviderProperties;
import io.mubel.provider.jdbc.eventstore.mysql.MysqlErrorMapper;
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
import io.mubel.server.spi.queue.QueueConfigurations;
import io.mubel.server.spi.support.IdGenerator;
import org.jdbi.v3.core.Jdbi;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;

public class EventStoreFactory {

    private static final String DEFAULT_DEADLINE_QUEUE_CONFIG_NAME = "deadlines";

    private final JdbcDataSources dataSources;
    private final JdbcProviderProperties properties;
    private final Scheduler scheduler;
    private final IdGenerator idGenerator;
    private final QueueConfigurations queueConfigurations;

    public EventStoreFactory(
            JdbcDataSources dataSources,
            JdbcProviderProperties properties,
            Scheduler scheduler,
            IdGenerator idGenerator,
            QueueConfigurations queueConfigurations
    ) {
        this.dataSources = dataSources;
        this.properties = properties;
        this.scheduler = scheduler;
        this.idGenerator = idGenerator;
        this.queueConfigurations = queueConfigurations;
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
                new MysqlErrorMapper()
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

        var queueConfig = queueConfigurations.getQueue("mysql", DEFAULT_DEADLINE_QUEUE_CONFIG_NAME);
        var messageQueueService = new JdbcMessageQueueService.Builder()
                .jdbi(jdbi)
                .statements(queueStatements)
                .idGenerator(idGenerator)
                .waitStrategy(new SimpleWaitStrategy(queueConfig.polIInterval()))
                .pollStrategy(new MysqlPollStrategy(queueStatements))
                .deleteStrategy(new BatchDeleteStrategy(queueStatements))
                .delayOffsetMs(-400)
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

        var queueConfig = queueConfigurations.getQueue("postgres", DEFAULT_DEADLINE_QUEUE_CONFIG_NAME);
        var messageQueueService = new JdbcMessageQueueService.Builder()
                .jdbi(jdbi)
                .statements(queueStatements)
                .idGenerator(idGenerator)
                .waitStrategy(new SimpleWaitStrategy(queueConfig.polIInterval()))
                .pollStrategy(new PgPollStrategy(queueStatements))
                .deleteStrategy(new DefaultDeleteStrategy(queueStatements))
                .delayOffsetMs(-500)
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
