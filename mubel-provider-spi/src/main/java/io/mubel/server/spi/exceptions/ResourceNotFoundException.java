package io.mubel.server.spi.exceptions;

public class ResourceNotFoundException extends BadRequestException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
