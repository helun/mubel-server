package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.SubscribeRequest;
import io.mubel.server.spi.DataStream;
import io.mubel.server.spi.ReplayService;
import org.jdbi.v3.core.Jdbi;

import java.util.concurrent.Executor;

public class JdbcReplayService implements ReplayService {

    private static final int BATCH_SIZE = 512;
    private final Jdbi jdbi;
    private final Executor executor;
    private final EventStoreStatements statements;

    public JdbcReplayService(Jdbi jdbi, EventStoreStatements statements, Executor executor) {
        this.jdbi = jdbi;
        this.executor = executor;
        this.statements = statements;
    }

    @Override
    public DataStream<EventData> replay(SubscribeRequest request) {
        final var stream = new DataStream<>(BATCH_SIZE, EventData.newBuilder().build());
        executor.execute(() -> replayInternal(request, stream));
        return stream;
    }

    private void replayInternal(SubscribeRequest request, DataStream<EventData> stream) {
        jdbi.useHandle(handle -> {
            try {
                handle.createQuery(statements.replaySql())
                        .bind(0, request.getFromSequenceNo())
                        .map(new EventDataRowMapper())
                        .useIterator(iter -> {
                            while (iter.hasNext()) {
                                var eventData = iter.next();
                                stream.put(eventData);
                            }
                            stream.end();
                        });
            } catch (RuntimeException e) {
                stream.fail(e);
            } catch (InterruptedException e) {
                stream.end();
                Thread.currentThread().interrupt();
            }
        });
    }
}
