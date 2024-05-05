package io.mubel.provider.inmemory;

import io.mubel.provider.inmemory.eventstore.InMemEventStores;
import io.mubel.provider.inmemory.eventstore.InMemReplayService;
import io.mubel.provider.inmemory.queue.InMemMessageQueueService;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.StorageBackendProperties;
import io.mubel.server.spi.support.AsyncExecuteRequestHandler;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemProvider implements Provider {

    public static final String PROVIDER_NAME = "inmemory";

    private final InMemEventStores eventStores;
    private final InMemReplayService replayService;
    private final InMemMessageQueueService messageQueueService;
    private final Map<String, AsyncExecuteRequestHandler> requesthandlers = new ConcurrentHashMap<>();

    public InMemProvider(
            InMemEventStores eventStores,
            InMemMessageQueueService messageQueueService
    ) {
        this.eventStores = eventStores;
        this.replayService = new InMemReplayService(eventStores);
        this.messageQueueService = messageQueueService;
    }

    @Override
    public String name() {
        return PROVIDER_NAME;
    }

    @Override
    public Set<StorageBackendProperties> storageBackends() {
        return Set.copyOf(eventStores.storageBackends());
    }

    @Override
    public void provision(ProvisionCommand command) {
        eventStores.create(eventStores.provision(command));
    }

    @Override
    public void drop(DropEventStoreCommand command) {
        eventStores.drop(command);
    }

    @Override
    public StorageBackendProperties getStorageBackend(String storageBackendName) {
        return eventStores.storageBackends().stream()
                .filter(backend -> backend.name().equals(storageBackendName))
                .findFirst()
                .orElseThrow(() -> new ResourceNotFoundException("Unknown backend " + storageBackendName));
    }

    @Override
    public EventStoreContext openEventStore(String esid) {
        var eventStore = eventStores.get(esid);
        var executeRequestHandler = new AsyncExecuteRequestHandler(
                esid,
                eventStore,
                messageQueueService,
                100,
                1000
        );
        executeRequestHandler.start();
        requesthandlers.put(esid, executeRequestHandler);
        return new EventStoreContext(
                esid,
                executeRequestHandler,
                eventStore,
                replayService,
                eventStore,
                messageQueueService
        );
    }

    @Override
    public void closeEventStore(String esid) {
        eventStores.close(esid);
        var rh = requesthandlers.remove(esid);
        if (rh != null) {
            rh.stop();
        }
    }
}
