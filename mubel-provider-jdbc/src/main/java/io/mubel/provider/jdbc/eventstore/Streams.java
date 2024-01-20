package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.EventDataInput;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Streams {

    private static final RowMapper<Map.Entry<String, Long>> ENTRY_ROW_MAPPER = (rs, ctx) -> Map.entry(rs.getString(1), rs.getLong(2));
    private final Jdbi jdbi;
    private final EventStoreStatements statements;

    public Streams(Jdbi jdbi, EventStoreStatements statements) {
        this.jdbi = jdbi;
        this.statements = statements;
    }

    public StreamIds lookupIds(AppendRequest request) {
        var ids = analyzeStreams(request.getEventList());
        ids.add(insertNewStreams(ids.getNewStreams()));
        ids.add(lookupIds(ids.getExistingStreams()));
        return ids;
    }

    private Map<String, Long> insertNewStreams(Collection<UUID> newStreams) {
        if (newStreams.isEmpty()) {
            return Map.of();
        }
        return jdbi.withHandle(h -> {
            var batch = h.prepareBatch(statements.insertStream());
            for (var streamId : newStreams) {
                batch.bind(0, streamId);
                batch.add();
            }
            return batch.executePreparedBatch("stream_id", "id")
                    .map(ENTRY_ROW_MAPPER)
                    .collectToMap(Map.Entry::getKey, Map.Entry::getValue);
        });
    }

    private Map<String, Long> lookupIds(Collection<UUID> streamIds) {
        if (streamIds.isEmpty()) {
            return Map.of();
        }
        return jdbi.withHandle(h ->
                h.createQuery(statements.selectStreamIds())
                        .bindList("streamIds", streamIds)
                        .map(ENTRY_ROW_MAPPER)
                        .collectToMap(Map.Entry::getKey, Map.Entry::getValue)
        );
    }

    private StreamIds analyzeStreams(Collection<EventDataInput> input) {
        final var streamIds = new HashMap<UUID, Boolean>(input.size());
        for (var event : input) {
            final var streamId = UUID.fromString(event.getStreamId());
            if (event.getVersion() == 0) {
                streamIds.put(streamId, true);
            } else {
                streamIds.putIfAbsent(streamId, false);
            }
        }
        return new StreamIds(streamIds);
    }

}
