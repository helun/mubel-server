package io.mubel.server.spi.eventstore;

import java.util.HashMap;
import java.util.Map;

public class Revisions {
    private final static Revisions EMPTY = new Revisions(0);
    private final Map<String, Integer> revisions;

    public Revisions(int initialCapacity) {
        revisions = new HashMap<>(initialCapacity);
    }

    public static Revisions empty() {
        return EMPTY;
    }

    public Revisions add(String streamId, int revision) {
        revisions.put(streamId, revision);
        return this;
    }

    public int nextRevision(String streamId) {
        return revisions.compute(streamId, (k, v) -> v == null ? 0 : v + 1);
    }

}
