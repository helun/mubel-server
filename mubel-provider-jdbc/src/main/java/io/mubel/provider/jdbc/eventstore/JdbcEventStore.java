package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.api.grpc.GetEventsResponse;
import io.mubel.server.spi.EventStore;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Query;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.mubel.schema.Constrains.isNotBlank;
import static io.mubel.schema.Constrains.requireNotBlank;

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
    public List<EventData> append(AppendRequest request) {
        try {
            return appendInternal(request);
        } catch (Exception e) {
            throw errorMapper.map(e);
        }
    }

    private List<EventData> appendInternal(AppendRequest request) {
        return jdbi.inTransaction(x -> {
            if (isNotBlank(request.getRequestId()) && !requestLog.log(UUID.fromString(request.getRequestId()))) {
                return List.of();
            }
            final var result = new ArrayList<EventData>(request.getEventCount());
            final var edb = EventData.newBuilder();
            jdbi.useHandle(h -> {
                final var batch = h.prepareBatch(statements.append());
                for (var ed : request.getEventList()) {
                    final var millis = clock.millis();
                    batch.bind(0, UUID.fromString(ed.getId()))
                            .bind(1, UUID.fromString(ed.getStreamId()))
                            .bind(2, ed.getVersion())
                            .bind(3, ed.getType())
                            .bind(4, millis)
                            .bind(5, ed.getData().toByteArray())
                            .bind(6, ed.getMetaData().toByteArray())
                            .add();
                    result.add(edb
                            .setId(ed.getId())
                            .setStreamId(ed.getStreamId())
                            .setVersion(ed.getVersion())
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
        return request.getStreamId().isBlank() ?
                getAll(request) :
                getByStream(request);
    }

    @Override
    public void truncate() {
        jdbi.useHandle(h -> h.createUpdate(statements.truncate()).execute());
    }

    public long maxSequenceNo() {
        return jdbi.withHandle(h -> h.createQuery(statements.getSequenceNoSql()).mapTo(Long.class).one());
    }

    private GetEventsResponse getByStream(GetEventsRequest request) {
        final var nnStreamId = requireNotBlank(request.getStreamId(), "streamId may not be null");
        final int sizeLimit = statements.parseSizeLimit(request.getSize());
        final var events = jdbi.withHandle(h -> {
            final Query query;
            if (request.getToVersion() == 0) {
                query = h.createQuery(statements.getSql())
                        .bind(0, UUID.fromString(nnStreamId))
                        .bind(1, request.getFromVersion())
                        .bind(2, sizeLimit);

            } else {
                query = h.createQuery(statements.getMaxVersionSql())
                        .bind(0, UUID.fromString(nnStreamId))
                        .bind(1, request.getFromVersion())
                        .bind(2, request.getToVersion())
                        .bind(3, sizeLimit);
            }
            return query.map(new EventDataRowMapper()).list();
        });
        return GetEventsResponse.newBuilder()
                .addAllEvent(events)
                .setSize(events.size())
                .setStreamId(nnStreamId)
                .build();
    }

    private GetEventsResponse getAll(GetEventsRequest request) {
        final var events = jdbi.withHandle(h ->
                h.createQuery(statements.pagedReplaySql())
                        .bind(0, request.getFromSequenceNo())
                        .bind(1, statements.parseSizeLimit(request.getSize()))
                        .map(new EventDataRowMapper())
                        .list());
        return GetEventsResponse.newBuilder()
                .addAllEvent(events)
                .setSize(events.size())
                .build();
    }
}
