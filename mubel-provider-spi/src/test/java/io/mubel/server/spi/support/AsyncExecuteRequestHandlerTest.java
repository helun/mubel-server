package io.mubel.server.spi.support;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.queue.MessageQueueService;
import io.mubel.server.spi.queue.SendRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AsyncExecuteRequestHandlerTest {

    public static final String ESID = "esid";
    @Mock
    EventStore eventStore;

    @Mock
    MessageQueueService messageQueueService;

    AsyncExecuteRequestHandler handler;

    @BeforeEach
    void setUp() {
        handler = new AsyncExecuteRequestHandler(ESID, eventStore, messageQueueService, 10, 1000);
    }

    @AfterEach
    void tearDown() {
        handler.stop();
    }

    @Test
    void all_operations() {
        handler.start();
        var cancelId = UUID.randomUUID();

        var request = ExecuteRequest.newBuilder()
                .setRequestId(ESID)
                .addOperation(appendOperation())
                .addOperation(cancelScheduledOperation(cancelId))
                .addOperation(scheduleDeadlineOperation())
                .addOperation(scheduleEventOperation())
                .build();

        assertThat(handler.handle(request))
                .succeedsWithin(Duration.ofMillis(100));
        verify(eventStore).append(request.getOperation(0).getAppend());
        verify(messageQueueService).delete(List.of(cancelId));
        verify(messageQueueService, times(2)).send(any(SendRequest.class));
    }

    @Test
    void queued_operations_are_joined_in_single_request() {
        var eventId1 = UUID.randomUUID().toString();
        var r1 = ExecuteRequest.newBuilder()
                .setRequestId(ESID)
                .addOperation(appendOperation(eventId1))
                .build();
        var eventId2 = UUID.randomUUID().toString();
        var r2 = ExecuteRequest.newBuilder()
                .setRequestId(ESID)
                .addOperation(appendOperation(eventId2))
                .build();
        var r1Future = handler.handle(r1);
        var r2Future = handler.handle(r2);
        assertThat(r1Future).isNotCompleted();
        assertThat(r2Future).isNotCompleted();
        handler.start();
        assertThat(r1Future).succeedsWithin(Duration.ofMillis(100));
        assertThat(r2Future).succeedsWithin(Duration.ofMillis(100));
        var captor = ArgumentCaptor.forClass(AppendOperation.class);
        verify(eventStore).append(captor.capture());
        assertThat(captor.getValue()
                .getEventList())
                .as("all events are appended in same operation")
                .hasSize(2)
                .map(EventDataInput::getId)
                .as("events are appended in queue order")
                .containsExactly(eventId1, eventId2);
    }

    private static Operation.Builder appendOperation() {
        return appendOperation(UUID.randomUUID().toString());
    }

    private static Operation.Builder appendOperation(String eventId) {
        return Operation.newBuilder()
                .setAppend(AppendOperation.newBuilder()
                        .addEvent(EventDataInput.newBuilder()
                                .setId(eventId)
                                .setType("test-type")
                                .setStreamId(UUID.randomUUID().toString())
                        )
                );
    }

    private static Operation.Builder cancelScheduledOperation(UUID cancelId) {
        return Operation.newBuilder()
                .setCancel(CancelScheduledOperation.newBuilder()
                        .addEventId(cancelId.toString())
                );
    }

    private static Operation.Builder scheduleDeadlineOperation() {
        return Operation.newBuilder()
                .setScheduleDeadline(ScheduleDeadlineOperation.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setDeadline(Deadline.newBuilder()
                                .setType("test-dl")
                                .setTargetEntity(EntityReference.newBuilder()
                                        .setId("test-id")
                                        .setType("test-entity")
                                )
                        )
                        .setPublishTime(System.currentTimeMillis() + 1000)
                );
    }

    private static Operation.Builder scheduleEventOperation() {
        return Operation.newBuilder()
                .setScheduleEvent(ScheduleEventOperation.newBuilder()
                        .setEvent(EventDataInput.newBuilder()
                                .setId(UUID.randomUUID().toString())
                                .setType("test-type")
                                .setStreamId(UUID.randomUUID().toString())
                        )
                        .setPublishTime(System.currentTimeMillis() + 1000)
                );
    }

}