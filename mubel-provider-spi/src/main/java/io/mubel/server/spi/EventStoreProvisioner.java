package io.mubel.server.spi;

import io.mubel.api.grpc.DropEventStoreRequest;
import io.mubel.api.grpc.ProvisionEventStoreRequest;

public interface EventStoreProvisioner {

    SpiEventStoreDetails provision(ProvisionEventStoreRequest request);

    void drop(DropEventStoreRequest request);
}
