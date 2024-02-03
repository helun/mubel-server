package io.mubel.server.spi;

import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.StorageBackendProperties;

import java.util.Set;

public interface Provider {

    String name();

    Set<StorageBackendProperties> storageBackends();

    /**
     * Provision an event store.
     * The provider is responsible for creating the event store.
     *
     * @param command
     */
    void provision(ProvisionCommand command);

    void drop(DropEventStoreCommand command);

    StorageBackendProperties getStorageBackend(String storageBackendName);

    EventStoreContext openEventStore(String esid);

    void closeEventStore(String esid);
}
