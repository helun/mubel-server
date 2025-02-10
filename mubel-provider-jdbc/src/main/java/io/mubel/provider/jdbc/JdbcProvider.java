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
package io.mubel.provider.jdbc;

import io.mubel.provider.jdbc.configuration.JdbcProviderProperties;
import io.mubel.provider.jdbc.eventstore.EventStoreFactory;
import io.mubel.provider.jdbc.eventstore.JdbcEventStoreContext;
import io.mubel.provider.jdbc.support.JdbcDataSources;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.eventstore.ExecuteRequestHandler;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.execute.BatchingExecuteRequestHandler;
import io.mubel.server.spi.groups.LeaderQueries;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.StorageBackendProperties;
import io.mubel.server.spi.scheduled.ScheduledEventsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class JdbcProvider implements Provider {

    public static final String PROVIDER_NAME = "jdbc";

    private final static Logger LOG = LoggerFactory.getLogger(JdbcProvider.class);

    private final Map<String, JdbcEventStoreContext> contexts = new ConcurrentHashMap<>();
    private final Map<String, ExecuteRequestHandler> requestHandlers = new ConcurrentHashMap<>();
    private final Map<String, ScheduledEventsHandler> scheduledEventHandlers = new ConcurrentHashMap<>();
    private final EventStoreFactory eventStoreFactory;
    private final JdbcDataSources jdbcDataSources;
    private final JdbcProviderProperties properties;
    private final LeaderQueries leaderQueries;

    public JdbcProvider(
            EventStoreFactory eventStoreFactory,
            JdbcDataSources jdbcDataSources,
            JdbcProviderProperties properties,
            LeaderQueries leaderQueries
    ) {
        this.eventStoreFactory = eventStoreFactory;
        this.jdbcDataSources = jdbcDataSources;
        this.properties = properties;
        this.leaderQueries = leaderQueries;
    }

    @Override
    public String name() {
        return PROVIDER_NAME;
    }

    @Override
    public Set<StorageBackendProperties> storageBackends() {
        return properties.getBackends()
                .stream()
                .map(bep -> new StorageBackendProperties(
                        bep.getName(),
                        jdbcDataSources.get(bep.getDataSource()).backendType(),
                        PROVIDER_NAME
                )).collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public StorageBackendProperties getStorageBackend(String storageBackendName) {
        return properties.findBackend(storageBackendName)
                .map(bep -> new StorageBackendProperties(
                        bep.getName(),
                        jdbcDataSources.get(bep.getDataSource()).backendType(),
                        PROVIDER_NAME
                )).orElseThrow(() -> new ResourceNotFoundException("No backend found for name: " + storageBackendName));
    }

    @Override
    public void provision(ProvisionCommand command) {
        var context = eventStoreFactory.create(command);
        contexts.put(command.esid(), context);
        context.provisioner().provision(command);
    }

    @Override
    public void drop(DropEventStoreCommand command) {
        LOG.info("dropping event store: {}", command.esid());
        var context = contexts.get(command.esid());
        context.provisioner().drop(command);
    }

    @Override
    public EventStoreContext openEventStore(String esid) {
        var jc = contexts.get(esid);

        var executeRequestHandler = initRequestHandler(esid, jc);
        initScheduledEventsHandler(esid, jc);
        return new EventStoreContext(
                esid,
                executeRequestHandler,
                jc.eventStore(),
                jc.replayService(),
                jc.liveEventsService(),
                jc.messageQueueService(),
                leaderQueries
        );
    }

    private void initScheduledEventsHandler(String esid, JdbcEventStoreContext jc) {
        var scheduledEventsHandler = new ScheduledEventsHandler(
                esid,
                jc.eventStore(),
                jc.messageQueueService()
        );
        scheduledEventsHandler.start();
        scheduledEventHandlers.put(esid, scheduledEventsHandler);
    }

    private ExecuteRequestHandler initRequestHandler(String esid, JdbcEventStoreContext jc) {
        var executeRequestHandler = new BatchingExecuteRequestHandler(
                esid,
                jc.eventStore(),
                jc.messageQueueService()
        );
        requestHandlers.put(esid, executeRequestHandler);
        return executeRequestHandler;
    }

    @Override
    public void closeEventStore(String esid) {
        LOG.info("closing event store: {}", esid);
        contexts.get(esid).close();
        Optional.ofNullable(requestHandlers.remove(esid))
                .ifPresent(ExecuteRequestHandler::stop);
        Optional.ofNullable(scheduledEventHandlers.remove(esid))
                .ifPresent(ScheduledEventsHandler::stop);
    }
}
