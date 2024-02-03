package io.mubel.server.spi.eventstore;

import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.SpiEventStoreDetails;

public interface EventStoreProvisioner {

    SpiEventStoreDetails provision(ProvisionCommand request);

    void drop(DropEventStoreCommand request);
}
