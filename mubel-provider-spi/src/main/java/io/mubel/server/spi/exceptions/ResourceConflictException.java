package io.mubel.server.spi.exceptions;

public class ResourceConflictException extends BadRequestException {

    public ResourceConflictException(String message) {
        super(message);
    }
}
