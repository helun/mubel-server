package io.mubel.server.spi.exceptions;

public class BackendException extends RuntimeException {

    public BackendException(String message, Throwable cause) {
        super(message, cause);
    }

    public BackendException(String message) {
        super(message);
    }
}
