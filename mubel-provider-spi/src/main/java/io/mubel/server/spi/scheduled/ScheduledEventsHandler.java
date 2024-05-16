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
                            .subscribe(this::handleEvents);
                } catch (Throwable err) {
                    LOG.error("Error while handling scheduled events", err);
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
        appendOperationBuilder.clear()
                .addAllEvent(events);
        eventStore.append(appendOperationBuilder.build());
        scheduledEventsQueue.delete(messageIds);
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

    private record MessageIdAndData(UUID messageId, EventDataInput data) {
    }

}
