package io.mubel.server;

import io.mubel.api.grpc.DataFormat;
import io.mubel.provider.inmemory.eventstore.InMemEventStore;
import io.mubel.provider.inmemory.eventstore.InMemEventStores;
import io.mubel.provider.inmemory.eventstore.InMemReplayService;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.eventstore.EventStoreState;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TestProvider implements Provider {

    public static final String TEST_BACKEND = "test-backend";
    private final List<ProvisionCommand> provisionCommands = new ArrayList<>();
    private final List<DropEventStoreCommand> dropCommands = new ArrayList<>();

    private final List<String> openEventStores = new ArrayList<>();
    private final List<String> closeEventStores = new ArrayList<>();

    private Set<StorageBackendProperties> backends = Set.of(
            new StorageBackendProperties(TEST_BACKEND, BackendType.IN_MEMORY, "test-backend")
    );


    @Override
    public String name() {
        return "test-provider";
    }

    @Override
    public Set<StorageBackendProperties> storageBackends() {
        return backends;
    }

    @Override
    public void provision(ProvisionCommand command) {
        provisionCommands.add(command);
    }

    @Override
    public void drop(DropEventStoreCommand command) {
        dropCommands.add(command);
    }

    @Override
    public StorageBackendProperties getStorageBackend(String storageBackendName) {
        return backends.stream()
                .filter(backend -> backend.name().equals(storageBackendName))
                .findFirst()
                .orElseThrow(() -> new ResourceNotFoundException("No backend found for name: " + storageBackendName));
    }

    @Override
    public EventStoreContext openEventStore(String esid) {
        openEventStores.add(esid);
        InMemEventStores eventStores = new InMemEventStores(Set.of(TEST_BACKEND));
        InMemEventStore eventStore = eventStores.create(new SpiEventStoreDetails(
                esid,
                "test-provider",
                BackendType.IN_MEMORY,
                DataFormat.JSON,
                EventStoreState.PROVISIONED
        ));
        return new EventStoreContext(
                esid,
                eventStore,
                new InMemReplayService(eventStores),
                eventStore
        );
    }

    @Override
    public void closeEventStore(String esid) {
        closeEventStores.add(esid);
    }

    public boolean hasProvisionCommand(ProvisionCommand command) {
        return provisionCommands.contains(command);
    }

    public boolean hasDropCommand(DropEventStoreCommand command) {
        return dropCommands.contains(command);
    }

    public boolean hasOpenEventStore(String esid) {
        return openEventStores.contains(esid);
    }

    public boolean hasCloseEventStore(String esid) {
        return closeEventStores.contains(esid);
    }
}
