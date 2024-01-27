package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.DropEventStoreRequest;
import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.server.spi.model.SpiEventStoreDetails;

public interface EventStoreProvisioner {

    SpiEventStoreDetails provision(ProvisionEventStoreRequest request);

    void drop(DropEventStoreRequest request);
}
