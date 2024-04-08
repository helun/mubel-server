package io.mubel.provider.jdbc.eventstore;

import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import io.mubel.server.spi.eventstore.ReplayService;
import org.jdbi.v3.core.Jdbi;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class JdbcReplayService implements ReplayService {

    private static final int BATCH_SIZE = 256;
    private final Jdbi jdbi;
    private final EventStoreStatements statements;
    private final Scheduler scheduler;

    public JdbcReplayService(
            Jdbi jdbi,
            EventStoreStatements statements,
            Scheduler scheduler
    ) {
        this.jdbi = jdbi;
        this.statements = statements;
        this.scheduler = scheduler;
    }

    @Override
    public Flux<EventData> replay(SubscribeRequest request) {
        return Flux.<EventData>push(sink -> replayInternal(request, sink))
                .subscribeOn(scheduler);
    }

    private void replayInternal(SubscribeRequest request, FluxSink<EventData> sink) {
        AtomicBoolean isCancelled = new AtomicBoolean(false);
        var selector = request.getSelector().getAll();
        if (selector == null) {
            throw new UnsupportedOperationException("Only AllSelector is supported");
        }
        AtomicLong lastSequenceNo = new AtomicLong(selector.getFromSequenceNo());
        sink.onCancel(() -> isCancelled.set(true));
        sink.onRequest(n -> jdbi.useHandle(handle -> {
            try {
                var limit = Math.min(n, BATCH_SIZE);
                handle.createQuery(statements.pagedReplaySql())
                        .bind(0, lastSequenceNo.get())
                        .bind(1, n)
                        .map(new EventDataRowMapper())
                        .useIterator(iter -> {
                            while (iter.hasNext() && !isCancelled.get()) {
                                var ed = iter.next();
                                lastSequenceNo.set(ed.getSequenceNo());
                                sink.next(ed);
                            }
                            sink.complete();
                        });
            } catch (RuntimeException e) {
                sink.error(e);
            }
        }));

    }
}
