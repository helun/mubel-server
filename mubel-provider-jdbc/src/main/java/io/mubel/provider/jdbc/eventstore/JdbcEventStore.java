package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.api.grpc.v1.server.EventStoreSummary;
import io.mubel.server.spi.eventstore.EventStore;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.result.ResultIterable;
import org.jdbi.v3.core.statement.Query;
import reactor.core.publisher.Flux;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.mubel.server.spi.support.Constraints.requireNotBlank;

public class JdbcEventStore implements EventStore {

    private final Jdbi jdbi;
    private final EventStoreStatements statements;
    private final RequestLog requestLog;
    private final Clock clock = Clock.systemUTC();
    private final ErrorMapper errorMapper;

    public JdbcEventStore(
            Jdbi jdbi,
            EventStoreStatements statements,
            ErrorMapper errorMapper
    ) {
        this.statements = statements;
        this.errorMapper = errorMapper;
        this.jdbi = jdbi;
        this.requestLog = new RequestLog(this.jdbi, statements);
    }

    public JdbcEventStore init() {
        return this;
    }

    @Override
    public List<EventData> append(AppendOperation request) {
        try {
            return appendInternal(request);
        } catch (Exception e) {
            throw errorMapper.map(e);
        }
    }

    private List<EventData> appendInternal(AppendOperation request) {
        return jdbi.inTransaction(x -> {
            /*
            if (isNotBlank(request.getRequestId()) && !requestLog.log(UUID.fromString(request.getRequestId()))) {
                return List.of();
            }
             */
            final var result = new ArrayList<EventData>(request.getEventCount());
            final var edb = EventData.newBuilder();
            jdbi.useHandle(h -> {
                final var batch = h.prepareBatch(statements.append());
                for (var ed : request.getEventList()) {
                    final var millis = clock.millis();
                    batch.bind(0, statements.convertUUID(ed.getId()))
                            .bind(1, statements.convertUUID(ed.getStreamId()))
                            .bind(2, ed.getRevision())
                            .bind(3, ed.getType())
                            .bind(4, millis)
                            .bind(5, ed.getData().toByteArray())
                            .bind(6, ed.getMetaData().toByteArray())
                            .add();
                    result.add(edb
                            .setId(ed.getId())
                            .setStreamId(ed.getStreamId())
                            .setRevision(ed.getRevision())
                            .setType(ed.getType())
                            .setCreatedAt(millis)
                            .setData(ed.getData())
                            .setMetaData(ed.getMetaData())
                            .build()
                    );
                }
                batch.execute();
            });
            return result;
        });
    }

    @Override
    public GetEventsResponse get(GetEventsRequest request) {
        return switch (request.getSelector().getByCase()) {
            case STREAM -> getByStream(request);
            case ALL, BY_NOT_SET -> getAll(request);
        };
    }

    @Override
    public Flux<EventData> getStream(GetEventsRequest request) {
        return switch (request.getSelector().getByCase()) {
            case STREAM -> streamByStream(request);
            case ALL, BY_NOT_SET -> streamAll(request);
        };
    }

    @Override
    public void truncate() {
        jdbi.useHandle(h -> statements.truncate().forEach(h::execute));
    }

    @Override
    public EventStoreSummary summary() {
        return jdbi.withHandle(h -> h.createQuery(statements.summarySql())
                .map((rs, ctx) -> EventStoreSummary.newBuilder()
                        .setEventCount(rs.getLong("event_count"))
                        .setStreamCount(rs.getLong("stream_count"))
                        .build())
                .one());
    }

    @Override
    public Map<String, Integer> getCurrentRevisions(List<String> streamIds) {
        if (streamIds.isEmpty()) {
            return Map.of();
        }
        return jdbi.withHandle(h -> {
                    var query = h.createQuery(statements.currentRevisionsSql(streamIds.size()));
                    for (var i = 0; i < streamIds.size(); i++) {
                        query.bind(i, statements.convertUUID(streamIds.get(i)));
                    }
                    return query.map((rs, ctx) -> Map.entry(rs.getString(1), rs.getInt(2)))
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                }
        );
    }

    public long maxSequenceNo() {
        return jdbi.withHandle(h -> h.createQuery(statements.getSequenceNoSql())
                .mapTo(Long.class)
                .one()
        );
    }

    private Flux<EventData> streamByStream(GetEventsRequest request) {
        return Flux.create(sink -> {
            try {
                final var selector = request.getSelector().getStream();
                final var nnStreamId = requireNotBlank(selector.getStreamId(), "streamId may not be null");
                final int sizeLimit = statements.parseSizeLimit(request.getSize());
                jdbi.useHandle(h -> {
                    setupByStreamQuery(h, selector, nnStreamId, sizeLimit)
                            .useStream(s -> s.forEach(sink::next));
                });
                sink.complete();
            } catch (Throwable e) {
                sink.error(e);
            }
        });
    }

    private GetEventsResponse getByStream(GetEventsRequest request) {
        var selector = request.getSelector().getStream();
        final var nnStreamId = requireNotBlank(selector.getStreamId(), "streamId may not be null");
        final int sizeLimit = statements.parseSizeLimit(request.getSize());
        final var events = jdbi.withHandle(h ->
                setupByStreamQuery(h, selector, nnStreamId, sizeLimit).list()
        );
        return GetEventsResponse.newBuilder()
                .addAllEvent(events)
                .setSize(events.size())
                .setStreamId(nnStreamId)
                .build();
    }

    private ResultIterable<EventData> setupByStreamQuery(Handle h, StreamSelector selector, String nnStreamId, int sizeLimit) {
        final Query query;
        if (selector.getToRevision() == 0) {
            query = h.createQuery(statements.getSql())
                    .bind(0, statements.convertUUID(nnStreamId))
                    .bind(1, selector.getFromRevision())
                    .bind(2, sizeLimit);

        } else {
            query = h.createQuery(statements.getMaxRevisionSql())
                    .bind(0, statements.convertUUID(nnStreamId))
                    .bind(1, selector.getFromRevision())
                    .bind(2, selector.getToRevision())
                    .bind(3, sizeLimit);
        }
        return query.map(new EventDataRowMapper());
    }

    private GetEventsResponse getAll(GetEventsRequest request) {
        final var selector = request.getSelector().getAll();
        final var events = jdbi.withHandle(h -> setupGetAllQuery(request, h, selector).list());
        return GetEventsResponse.newBuilder()
                .addAllEvent(events)
                .setSize(events.size())
                .build();
    }

    private Flux<EventData> streamAll(GetEventsRequest request) {
        return Flux.create(sink -> {
            try {
                final var selector = request.getSelector().getAll();
                jdbi.useHandle(h -> {
                    setupGetAllQuery(request, h, selector).useStream(s -> s.forEach(sink::next));
                });
                sink.complete();
            } catch (Throwable e) {
                sink.error(e);
            }
        });
    }

    private ResultIterable<EventData> setupGetAllQuery(GetEventsRequest request, Handle h, AllSelector selector) {
        return h.createQuery(statements.pagedReplaySql())
                .bind(0, selector.getFromSequenceNo())
                .bind(1, statements.parseSizeLimit(request.getSize()))
                .map(new EventDataRowMapper());
    }
}
