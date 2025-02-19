/*
 * mubel-provider-inmemory - Multi Backend Event Log
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mubel.provider.inmemory.eventstore;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.api.grpc.v1.server.EventStoreSummary;
import io.mubel.server.spi.ErrorMessages;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import io.mubel.server.spi.eventstore.Revisions;
import io.mubel.server.spi.exceptions.EventRevisionConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class InMemEventStore implements EventStore, LiveEventsService {

    private static final Logger LOG = LoggerFactory.getLogger(InMemEventStore.class);

    private final List<EventData> events = Collections.synchronizedList(new ArrayList<>(1000));
    private final Set<String> appendLog = new ConcurrentSkipListSet<>();
    private final Set<String> revisionLog = new ConcurrentSkipListSet<>();
    private final AtomicLong sequenceNo = new AtomicLong(0L);
    private final Clock clock = Clock.systemUTC();

    private FluxSink<EventData> liveSink;
    private final AtomicReference<Flux<EventData>> liveEvents = new AtomicReference<>(null);

    @Override
    public GetEventsResponse get(GetEventsRequest request) {
        final var result = setupEventDataStream(request).toList();
        return GetEventsResponse.newBuilder()
                .addAllEvent(result)
                .setSize(result.size())
                .build();
    }

    private Stream<EventData> setupEventDataStream(GetEventsRequest request) {
        var stream = switch (request.getSelector().getByCase()) {
            case STREAM -> getByStream(request.getSelector().getStream());
            case ALL -> getAll(request.getSelector().getAll());
            case BY_NOT_SET -> getAll(AllSelector.getDefaultInstance());
        };

        if (request.getSize() > 0) {
            stream = stream.limit(request.getSize());
        }
        return stream;
    }

    @Override
    public Flux<EventData> getStream(GetEventsRequest validated) {
        return Flux.fromStream(setupEventDataStream(validated));
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
        LOG.trace("appending {} events", request.getEventCount());
        final var eb = EventData.newBuilder();
        final var result = new ArrayList<EventData>(request.getEventCount());
        for (final var input : request.getEventList()) {
            if (!appendLog.add(input.getId())) {
                LOG.trace("Event with id {} already exists", input.getId());
                continue;
            }
            if (!revisionLog.add(input.getStreamId() + input.getRevision())) {
                throw new EventRevisionConflictException(
                        ErrorMessages.eventRevisionConflict(input.getStreamId(), input.getRevision())
                );
            }
            final var ed = eb.setData(input.getData())
                    .setId(input.getId())
                    .setStreamId(input.getStreamId())
                    .setCreatedAt(clock.millis())
                    .setType(input.getType())
                    .setRevision(input.getRevision())
                    .setSequenceNo(sequenceNo.incrementAndGet())
                    .setMetaData(input.getMetaData())
                    .build();
            result.add(ed);
        }
        events.addAll(result);
        LOG.trace("appended {} events, events size: {}", result.size(), events.size());
        publishLive(result);
        return result;
    }

    private void publishLive(ArrayList<EventData> result) {
        LOG.trace("publishing {} events to live stream", result.size());
        if (liveSink != null) {
            for (var event : result) {
                liveSink.next(event);
            }
        }
    }

    @Override
    public Flux<EventData> liveEvents() {
        return liveEvents.updateAndGet(f -> Objects.requireNonNullElseGet(f, this::initLiveEvents));
    }

    private Flux<EventData> initLiveEvents() {
        return Flux.<EventData>create(sink -> {
            liveSink = sink;
            sink.onDispose(() -> liveSink = null);
        }).share();
    }

    @Override
    public EventStoreSummary summary() {
        return EventStoreSummary.newBuilder()
                .setEventCount(events.size())
                .setStreamCount(events.stream().map(EventData::getStreamId).distinct().count())
                .build();
    }

    @Override
    public Revisions getRevisions(List<String> streamIds) {
        if (streamIds.isEmpty()) {
            return Revisions.empty();
        }
        return events.stream()
                .filter(event -> streamIds.contains(event.getStreamId()))
                .reduce(new Revisions(streamIds.size()),
                        (revisions, event) -> revisions.add(event.getStreamId(), event.getRevision()),
                        (r1, r2) -> r1
                );
    }
}
