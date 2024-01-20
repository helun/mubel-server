package io.mubel.server.spi;

import io.mubel.api.grpc.DataFormat;

public record SpiEventStoreDetails(
        String esid,
        String provider,
        BackendType type,
        DataFormat dataFormat,
        EventStoreState state
) {
}
