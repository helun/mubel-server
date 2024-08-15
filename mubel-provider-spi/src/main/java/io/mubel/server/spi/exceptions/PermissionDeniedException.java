package io.mubel.server.spi.exceptions;

public class PermissionDeniedException extends BadRequestException {
    
    public PermissionDeniedException(String message) {
        super(message);
    }
}
