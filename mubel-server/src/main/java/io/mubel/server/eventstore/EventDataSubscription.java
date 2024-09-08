package io.mubel.server.eventstore;

import io.mubel.api.grpc.v1.events.AllSelector;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.EventSelector;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.exceptions.PermissionDeniedException;
import io.mubel.server.spi.exceptions.SequenceNoOutOfSyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public final class EventDataSubscription {

    private static final Logger LOG = LoggerFactory.getLogger(EventDataSubscription.class);

    private EventDataSubscription() {
    }

    public static Flux<EventData> setupSubscription(
            SubscribeRequest request,
            EventStoreContext context
    ) {
        if (request.hasConsumerGroupToken() && !checkLeadership(request, context)) {
            LOG.warn("Subscription rejected: Client is not the leader of a consumer group");
            return Flux.error(new PermissionDeniedException("Subscription rejected: Client is not the leader of the consumer group."));
        }
        final var allSelector = request.getSelector().getAll();
        LOG.debug("Setting up subscription for {} from sequence no {}", request.getEsid(), allSelector.getFromSequenceNo());
        final var lastSequenceNo = new AtomicLong(allSelector.getFromSequenceNo());
        LOG.debug("Last sequence no: {}", lastSequenceNo.get());
        return context.replayService()
                .replay(request)
                .concatWith(context.liveEventsService().liveEvents())
                .handle(checkSequence(request, lastSequenceNo))
                .onErrorResume(shouldResume(), e -> resume(request, context, lastSequenceNo));
    }

    private static boolean checkLeadership(SubscribeRequest request, EventStoreContext context) {
        final var token = request.getConsumerGroupToken();
        return context.leaderQueries().isLeader(token);
    }

    private static Flux<EventData> resume(SubscribeRequest request, EventStoreContext context, AtomicLong lastSequenceNo) {
        long resumeFrom = lastSequenceNo.get();
        LOG.warn("Resuming subscription from {}", resumeFrom);
        return setupSubscription(request.toBuilder()
                .setSelector(EventSelector.newBuilder()
                        .setAll(
                                AllSelector.newBuilder().setFromSequenceNo(resumeFrom)
                        )
                )
                .build(), context);
    }

    private static Predicate<Throwable> shouldResume() {
        return err -> err instanceof SequenceNoOutOfSyncException;
    }

    private static BiConsumer<EventData, SynchronousSink<EventData>> checkSequence(SubscribeRequest request, AtomicLong lastSequenceNo) {
        return (EventData ed, SynchronousSink<EventData> sink) -> {
            final long sequenceNo = ed.getSequenceNo();
            final long expected = lastSequenceNo.get() + 1;
            if (sequenceNo != expected) {
                LOG.warn("Sequence no out of sync: {} was {} expected {}", request.getEsid(), sequenceNo, expected);
                sink.error(new SequenceNoOutOfSyncException(request.getEsid(), expected, sequenceNo));
            } else {
                sink.next(ed);
                LOG.trace("Sequence no {} passed for {}", sequenceNo, request.getEsid());
                lastSequenceNo.set(sequenceNo);
            }
        };
    }

}
