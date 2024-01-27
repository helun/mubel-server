package io.mubel.server.spi.exceptions;

public class MubelConfigurationException extends BackendException {

    public MubelConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    public MubelConfigurationException(String message) {
        super(message);
    }
}
