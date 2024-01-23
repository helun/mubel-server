package io.mubel.server.spi.exceptions;

public class SlowConsumerException extends BackendException {

    public SlowConsumerException() {
        super("Slow consumer");
    }
}
