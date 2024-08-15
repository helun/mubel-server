package io.mubel.provider.inmemory;

import io.mubel.provider.inmemory.eventstore.InMemEventStore;
import io.mubel.provider.inmemory.eventstore.InMemEventStores;
import io.mubel.provider.inmemory.eventstore.InMemReplayService;
import io.mubel.provider.inmemory.queue.InMemMessageQueueService;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.execute.AsyncExecuteRequestHandler;
import io.mubel.server.spi.groups.LeaderQueries;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.StorageBackendProperties;
import io.mubel.server.spi.scheduled.ScheduledEventsHandler;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InMemProvider implements Provider {

    public static final String PROVIDER_NAME = "inmemory";

    private final InMemEventStores eventStores;
    private final InMemReplayService replayService;
    private final InMemMessageQueueService messageQueueService;
    private final Map<String, AsyncExecuteRequestHandler> requesthandlers = new ConcurrentHashMap<>();
    private final Map<String, ScheduledEventsHandler> scheduledEventHandlers = new ConcurrentHashMap<>();
    private final LeaderQueries leaderQueries;

    public InMemProvider(
            InMemEventStores eventStores,
            InMemMessageQueueService messageQueueService,
            LeaderQueries leaderQueries
    ) {
        this.eventStores = eventStores;
        this.replayService = new InMemReplayService(eventStores);
        this.messageQueueService = messageQueueService;
        this.leaderQueries = leaderQueries;
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
        var executeRequestHandler = initRequestHandler(esid, eventStore);
        initScheduledEventsHandler(esid, eventStore);
        return new EventStoreContext(
                esid,
                executeRequestHandler,
                eventStore,
                replayService,
                eventStore,
                messageQueueService,
                leaderQueries
        );
    }

    private void initScheduledEventsHandler(String esid, InMemEventStore eventStore) {
        var scheduledEventsHandler = new ScheduledEventsHandler(
                esid,
                eventStore,
                messageQueueService
        );
        scheduledEventsHandler.start();
        scheduledEventHandlers.put(esid, scheduledEventsHandler);
    }

    private AsyncExecuteRequestHandler initRequestHandler(String esid, EventStore eventStore) {
        var executeRequestHandler = new AsyncExecuteRequestHandler(
                esid,
                eventStore,
                messageQueueService,
                100,
                1000
        );
        executeRequestHandler.start();
        requesthandlers.put(esid, executeRequestHandler);
        return executeRequestHandler;
    }

    @Override
    public void closeEventStore(String esid) {
        eventStores.close(esid);
        Optional.ofNullable(requesthandlers.remove(esid)).ifPresent(AsyncExecuteRequestHandler::stop);
        Optional.ofNullable(scheduledEventHandlers.remove(esid)).ifPresent(ScheduledEventsHandler::stop);
    }
}
