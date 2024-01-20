package io.mubel.provider.jdbc.eventstore;

import java.util.*;

public class StreamIds {

    private Map<String, Long> dbIdByStreamId;
    private Collection<UUID> newStreams;
    private Collection<UUID> existingStreams;

    public StreamIds(Map<UUID, Boolean> newOrExistingStreams) {
        this.dbIdByStreamId = new HashMap<>(newOrExistingStreams.size());
        this.newStreams = new ArrayList<>(newOrExistingStreams.size());
        this.existingStreams = new ArrayList<>(newOrExistingStreams.size());
        for (var entry : newOrExistingStreams.entrySet()) {
            final var streamId = entry.getKey();
            final var isNew = entry.getValue();
            if (isNew) {
                newStreams.add(streamId);
            } else {
                existingStreams.add(streamId);
            }
        }
    }

    public Long getDbId(String streamId) {
        return dbIdByStreamId.get(streamId);
    }

    public Collection<UUID> getNewStreams() {
        return newStreams;
    }

    public Collection<UUID> getExistingStreams() {
        return existingStreams;
    }

    public void add(Map<String, Long> stringLongMap) {
        dbIdByStreamId.putAll(stringLongMap);
    }
}
