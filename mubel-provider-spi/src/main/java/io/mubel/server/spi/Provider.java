package io.mubel.server.spi;

import io.mubel.api.grpc.DropEventStoreResponse;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.StorageBackendProperties;

import java.util.Set;

public interface Provider {

    String name();

    Set<StorageBackendProperties> storageBackends();

    void provision(ProvisionCommand command);

    DropEventStoreResponse drop(DropEventStoreCommand command);

    StorageBackendProperties getStorageBackend(String storageBackendName);

}
