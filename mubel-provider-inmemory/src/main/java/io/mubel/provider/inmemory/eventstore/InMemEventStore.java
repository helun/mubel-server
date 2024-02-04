package io.mubel.provider.inmemory.eventstore;

import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.api.grpc.GetEventsResponse;
import io.mubel.server.spi.ErrorMessages;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import io.mubel.server.spi.exceptions.EventVersionConflictException;
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

import static io.mubel.schema.Constrains.isNotBlank;

public class InMemEventStore implements EventStore, LiveEventsService {

    private final List<EventData> events = Collections.synchronizedList(new ArrayList<>(1000));
    private final Set<String> appendLog = new ConcurrentSkipListSet<>();
    private final Set<String> versionLog = new ConcurrentSkipListSet<>();
    private final AtomicLong sequenceNo = new AtomicLong(0L);
    private final Clock clock = Clock.systemUTC();

    private FluxSink<EventData> liveSink;
    private Flux<EventData> liveEvents;

    @Override
    public GetEventsResponse get(GetEventsRequest request) {
        var stream = isNotBlank(request.getStreamId()) ?
                getByStream(request) :
                getAll(request);

        if (request.getSize() > 0) {
            stream = stream.limit(request.getSize());
        }
        final var result = stream.toList();
        return GetEventsResponse.newBuilder()
                .addAllEvent(result)
                .setSize(result.size())
                .build();
    }

    private Stream<EventData> getByStream(GetEventsRequest request) {
        Predicate<EventData> filter = (event) -> event.getStreamId().equals(request.getStreamId());
        if (request.getFromVersion() > 0) {
            filter = filter.and((event -> event.getVersion() >= request.getFromVersion()));
        }
        if (request.getToVersion() > 0) {
            filter = filter.and((event -> event.getVersion() <= request.getToVersion()));
        }
        return events.stream().filter(filter);
    }

    private Stream<EventData> getAll(GetEventsRequest request) {
        var stream = events.stream();
        if (request.getFromSequenceNo() > 0) {
            stream = stream.skip(request.getFromSequenceNo());
        }
        return stream;
    }

    @Override
    public void truncate() {
        events.clear();
        appendLog.clear();
        versionLog.clear();
        sequenceNo.set(0);
    }

    @Override
    public List<EventData> append(AppendRequest request) {
        final var eb = EventData.newBuilder();
        final var result = new ArrayList<EventData>(request.getEventCount());
        for (final var input : request.getEventList()) {
            if (!appendLog.add(input.getId())) {
                continue;
            }
            if (!versionLog.add(input.getStreamId() + input.getVersion())) {
                throw new EventVersionConflictException(
                        ErrorMessages.eventVersionConflict(input.getStreamId(), input.getVersion())
                );
            }
            final var ed = eb.setData(input.getData())
                    .setId(input.getId())
                    .setStreamId(input.getStreamId())
                    .setCreatedAt(clock.millis())
                    .setType(input.getType())
                    .setVersion(input.getVersion())
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
}
