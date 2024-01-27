package io.mubel.server.spi.model;

public record DropEventStoreCommand(
        String jobId,
        String esid
) {

}
