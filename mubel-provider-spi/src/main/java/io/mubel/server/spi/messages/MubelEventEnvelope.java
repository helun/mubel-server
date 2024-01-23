package io.mubel.server.spi.messages;

import org.springframework.context.ApplicationEvent;

public class MubelEventEnvelope extends ApplicationEvent {

    private final MubelEvents event;

    public MubelEventEnvelope(Object source, MubelEvents event) {
        super(source);
        this.event = event;
    }

    public MubelEvents event() {
        return event;
    }
}
