/*
 * mubel-provider-spi - Multi Backend Event Log
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
package io.mubel.server.spi;

import io.mubel.api.grpc.v1.events.AppendOperation;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.GetEventsRequest;
import io.mubel.api.grpc.v1.events.GetEventsResponse;
import io.mubel.api.grpc.v1.server.EventStoreSummary;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.Revisions;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestEventStore implements EventStore {

    private final AtomicBoolean holdAppendRequests = new AtomicBoolean(false);
    private final Semaphore appendSemaphore = new Semaphore(0);
    private final AtomicInteger sequenceNo = new AtomicInteger(0);
    private final List<AppendOperation> appendOperations = new CopyOnWriteArrayList<>();

    public AppendOperation firstAppendOperation() {
        return appendOperations.getFirst();
    }

    public AppendOperation lastAppendOperation() {
        return appendOperations.getLast();
    }

    @Override
    public List<EventData> append(AppendOperation operation) {
        if (holdAppendRequests.get()) {
            try {
                appendSemaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        var eb = EventData.newBuilder();
        appendOperations.add(operation);
        return operation.getEventList().stream()
                .map(input -> eb.setData(input.getData())
                        .setId(input.getId())
                        .setStreamId(input.getStreamId())
                        .setCreatedAt(System.currentTimeMillis())
                        .setType(input.getType())
                        .setRevision(input.getRevision())
                        .setSequenceNo(sequenceNo.incrementAndGet())
                        .setMetaData(input.getMetaData())
                        .build())
                .toList();
    }

    @Override
    public GetEventsResponse get(GetEventsRequest request) {
        return null;
    }

    @Override
    public Flux<EventData> getStream(GetEventsRequest validated) {
        return Flux.empty();
    }

    @Override
    public Revisions getRevisions(List<String> streamIds) {
        return null;
    }

    @Override
    public void truncate() {

    }

    @Override
    public EventStoreSummary summary() {
        return null;
    }

}
