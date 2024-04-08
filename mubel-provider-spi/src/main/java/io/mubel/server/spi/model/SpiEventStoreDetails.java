package io.mubel.server.spi.model;

import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.server.spi.eventstore.EventStoreState;

public record SpiEventStoreDetails(
        String esid,
        String provider,
        BackendType type,
        DataFormat dataFormat,
        EventStoreState state
) {

    public SpiEventStoreDetails withState(EventStoreState state) {
        return new SpiEventStoreDetails(esid, provider, type, dataFormat, state);
    }
}
