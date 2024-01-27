package io.mubel.server.spi.model;

public record StorageBackendProperties(
        String name,
        BackendType type,
        String provider
) {

}
