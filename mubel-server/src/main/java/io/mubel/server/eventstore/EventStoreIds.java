package io.mubel.server.eventstore;

import org.springframework.stereotype.Component;

@Component
public class EventStoreIds {

    public String resolveEventStoreId(String esid) {
        return esid;
    }

}
