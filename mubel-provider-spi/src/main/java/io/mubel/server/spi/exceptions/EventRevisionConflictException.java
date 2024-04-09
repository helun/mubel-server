package io.mubel.server.spi.exceptions;


public class EventRevisionConflictException extends BadRequestException {

    public EventRevisionConflictException(String detail) {
        super(detail);
    }
}
