package io.mubel.server.spi.model;

import io.mubel.api.grpc.v1.server.DataFormat;

public record ProvisionCommand(
        String jobId,
        String esid,
        DataFormat dataFormat,
        String storageBackendName
) {

}
