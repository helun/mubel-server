package io.mubel.provider.inmemory.eventstore;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.api.grpc.v1.server.EventStoreSummary;
import io.mubel.server.spi.ErrorMessages;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import io.mubel.server.spi.exceptions.EventRevisionConflictException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class InMemEventStore implements EventStore, LiveEventsService {

    private final List<EventData> events = Collections.synchronizedList(new ArrayList<>(1000));
    private final Set<String> appendLog = new ConcurrentSkipListSet<>();
    private final Set<String> revisionLog = new ConcurrentSkipListSet<>();
    private final AtomicLong sequenceNo = new AtomicLong(0L);
    private final Clock clock = Clock.systemUTC();

    private FluxSink<EventData> liveSink;
    private Flux<EventData> liveEvents;

    @Override
    public GetEventsResponse get(GetEventsRequest request) {
        var stream = switch (request.getSelector().getByCase()) {
            case STREAM -> getByStream(request.getSelector().getStream());
            case ALL -> getAll(request.getSelector().getAll());
            case BY_NOT_SET -> getAll(AllSelector.getDefaultInstance());
        };

        if (request.getSize() > 0) {
            stream = stream.limit(request.getSize());
        }
        final var result = stream.toList();
        return GetEventsResponse.newBuilder()
                .addAllEvent(result)
                .setSize(result.size())
                .build();
    }

    private Stream<EventData> getByStream(StreamSelector selector) {
        Predicate<EventData> filter = (event) -> event.getStreamId().equals(selector.getStreamId());
        if (selector.hasFromRevision()) {
            filter = filter.and((event -> event.getRevision() >= selector.getFromRevision()));
        }
        if (selector.hasToRevision()) {
            filter = filter.and((event -> event.getRevision() <= selector.getToRevision()));
        }
        return events.stream().filter(filter);
    }

    private Stream<EventData> getAll(AllSelector selector) {
        var stream = events.stream();
        if (selector.getFromSequenceNo() > 0) {
            stream = stream.skip(selector.getFromSequenceNo());
        }
        return stream;
    }

    @Override
    public void truncate() {
        events.clear();
        appendLog.clear();
        revisionLog.clear();
        sequenceNo.set(0);
    }

    @Override
    public List<EventData> append(AppendOperation request) {
        final var eb = EventData.newBuilder();
        final var result = new ArrayList<EventData>(request.getEventCount());
        for (final var input : request.getEventList()) {
            if (!appendLog.add(input.getId())) {
                continue;
            }
            if (!revisionLog.add(input.getStreamId() + input.getRevision())) {
                throw new EventRevisionConflictException(
                        ErrorMessages.eventVersionConflict(input.getStreamId(), input.getRevision())
                );
            }
            final var ed = eb.setData(input.getData())
                    .setId(input.getId())
                    .setStreamId(input.getStreamId())
                    .setCreatedAt(clock.millis())
                    .setType(input.getType())
                    .setRevision(input.getRevision())
                    .setSequenceNo(sequenceNo.incrementAndGet())
                    .build();
            result.add(ed);
        }
        events.addAll(result);
        publishLive(result);
        return result;
    }

    private void publishLive(ArrayList<EventData> result) {
        if (liveSink != null) {
            for (var event : result) {
                liveSink.next(event);
            }
        }
    }

    @Override
    public Flux<EventData> liveEvents() {
        if (liveSink == null) {
            liveEvents = Flux.create(sink -> {
                liveSink = sink;
                sink.onDispose(() -> liveSink = null);
            });

        }
        return liveEvents.share();
    }

    @Override
    public EventStoreSummary summary() {
        return EventStoreSummary.newBuilder()
                .setEventCount(events.size())
                .setStreamCount(events.stream().map(EventData::getStreamId).distinct().count())
                .build();
    }
}
