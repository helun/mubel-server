package io.mubel.server.mubelserver.eventstore;

import org.springframework.stereotype.Component;

@Component
public class EventStoreIds {

    public String resolveEventStoreId(String esid) {
        return esid;
    }

}
