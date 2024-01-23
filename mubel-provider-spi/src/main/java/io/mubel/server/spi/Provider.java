package io.mubel.server.spi;

import io.mubel.api.grpc.DropEventStoreRequest;
import io.mubel.api.grpc.DropEventStoreResponse;
import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.api.grpc.StorageBackendInfo;
import io.mubel.server.spi.eventstore.SpiEventStoreDetails;

import java.util.Set;

public interface Provider {

    String name();

    Set<StorageBackendInfo> storageBackends();

    SpiEventStoreDetails provision(ProvisionEventStoreRequest request);

    DropEventStoreResponse drop(DropEventStoreRequest request);
}
