package io.mubel.server.spi.exceptions;

public class StorageBackendException extends RuntimeException {

    public StorageBackendException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageBackendException(String message) {
        super(message);
    }
}
