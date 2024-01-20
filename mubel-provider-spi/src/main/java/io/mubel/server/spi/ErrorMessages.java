package io.mubel.server.spi;

public class ErrorMessages {

    public static String eventVersionConflict(String streamId, int version) {
        return "event: streamId: %s, version: %d already exists".formatted(streamId, version);
    }

}
