package io.mubel.provider.jdbc;

import io.mubel.api.grpc.DropEventStoreResponse;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.StorageBackendProperties;

import java.util.Set;

public class JdbcProvider implements Provider {

    public static final String PROVIDER_NAME = "jdbc";

    @Override
    public String name() {
        return PROVIDER_NAME;
    }

    @Override
    public Set<StorageBackendProperties> storageBackends() {
        return null;
    }

    @Override
    public StorageBackendProperties getStorageBackend(String storageBackendName) {
        return null;
    }

    @Override
    public void provision(ProvisionCommand command) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DropEventStoreResponse drop(DropEventStoreCommand command) {
        throw new UnsupportedOperationException();
    }
}
