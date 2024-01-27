package io.mubel.server.spi.messages;

import org.springframework.context.ApplicationEvent;

public class EventStoreEventEnvelope extends ApplicationEvent {

    private final EventStoreEvents event;

    public EventStoreEventEnvelope(Object source, EventStoreEvents event) {
        super(source);
        this.event = event;
    }

    public EventStoreEvents event() {
        return event;
    }
}
