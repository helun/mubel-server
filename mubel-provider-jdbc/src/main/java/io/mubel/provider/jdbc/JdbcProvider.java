package io.mubel.provider.jdbc;

import io.mubel.provider.jdbc.configuration.JdbcProviderProperties;
import io.mubel.provider.jdbc.eventstore.EventStoreFactory;
import io.mubel.provider.jdbc.eventstore.JdbcEventStoreContext;
import io.mubel.provider.jdbc.support.JdbcDataSources;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.StorageBackendProperties;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class JdbcProvider implements Provider {

    public static final String PROVIDER_NAME = "jdbc";

    private final Map<String, JdbcEventStoreContext> contexts = new ConcurrentHashMap<>();
    private final EventStoreFactory eventStoreFactory;
    private final JdbcDataSources jdbcDataSources;
    private final JdbcProviderProperties properties;

    public JdbcProvider(EventStoreFactory eventStoreFactory, JdbcDataSources jdbcDataSources, JdbcProviderProperties properties) {
        this.eventStoreFactory = eventStoreFactory;
        this.jdbcDataSources = jdbcDataSources;
        this.properties = properties;
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
        var context = contexts.get(command.esid());
        context.provisioner().drop(command);
    }

    @Override
    public EventStoreContext openEventStore(String esid) {
        var jc = contexts.get(esid);
        return new EventStoreContext(
                esid,
                jc.eventStore(),
                jc.replayService(),
                jc.liveEventsService()
        );
    }

    @Override
    public void closeEventStore(String esid) {
        contexts.get(esid).close();
    }
}
