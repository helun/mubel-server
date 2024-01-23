package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.DataFormat;
import io.mubel.server.spi.BackendType;

public record SpiEventStoreDetails(
        String esid,
        String provider,
        BackendType type,
        DataFormat dataFormat,
        EventStoreState state
) {
}
