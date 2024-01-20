package io.mubel.server.spi.exceptions;


public class EventVersionConflictException extends BadRequestException {

    public EventVersionConflictException(String detail) {
        super(detail);
    }
}
