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
package io.mubel.server.spi.scheduled;

import com.google.protobuf.InvalidProtocolBufferException;
import io.mubel.api.grpc.v1.events.AppendOperation;
import io.mubel.api.grpc.v1.events.EventDataInput;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.queue.Message;
import io.mubel.server.spi.queue.MessageQueueService;
import io.mubel.server.spi.queue.ReceiveRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ScheduledEventsHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledEventsHandler.class);

    private final Executor worker;
    private final EventStore eventStore;
    private final MessageQueueService scheduledEventsQueue;
    private final ReceiveRequest request;
    private volatile boolean shouldRun = true;
    private final AppendOperation.Builder appendOperationBuilder = AppendOperation.newBuilder();

    public ScheduledEventsHandler(
            String esid,
            EventStore eventStore,
            MessageQueueService scheduledEventsQueue
    ) {
        this.eventStore = eventStore;
        this.scheduledEventsQueue = scheduledEventsQueue;
        this.request = new ReceiveRequest(esid + "-sc", Duration.ofSeconds(20));
        worker = Executors.newSingleThreadExecutor(r -> new Thread(r, esid + "-scheduled_events-worker"));
    }

    public void start() {
        worker.execute(() -> {
            while (shouldRun) {
                try {
                    scheduledEventsQueue.receive(request)
                            .buffer(Duration.ofSeconds(1))
                            .subscribe(this::handleEvents, err -> LOG.error("error while handling scheduled events", err));
                } catch (Throwable err) {
                    LOG.error("error while handling scheduled events", err);
                    shouldRun = false;
                    break;
                }
            }
        });
    }

    private void handleEvents(List<Message> messages) {
        if (messages.isEmpty()) {
            return;
        }
        var events = new ArrayList<EventDataInput>(messages.size());
        var messageIds = new ArrayList<UUID>(messages.size());
        for (var msg : messages) {
            events.add(parseEventDataInput(msg));
            messageIds.add(msg.messageId());
        }
        var streamIds = new ArrayList<String>(events.size());
        for (var event : events) {
            streamIds.add(event.getStreamId());
        }
        var revisions = eventStore.getRevisions(streamIds);
        for (int i = 0; i < events.size(); i++) {
            var event = events.get(i);
            var modified = event.toBuilder()
                    .setRevision(revisions.nextRevision(event.getStreamId()))
                    .build();
            events.set(i, modified);
        }
        appendOperationBuilder.clear()
                .addAllEvent(events);
        eventStore.append(appendOperationBuilder.build());
        scheduledEventsQueue.delete(messageIds);
        LOG.debug("appended {} scheduled events", events.size());
    }

    private static EventDataInput parseEventDataInput(Message msg) {
        try {
            return EventDataInput.parseFrom(msg.payload());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        shouldRun = false;
    }
}
