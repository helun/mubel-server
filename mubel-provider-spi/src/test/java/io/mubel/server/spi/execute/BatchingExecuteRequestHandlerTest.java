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
package io.mubel.server.spi.execute;

import io.mubel.api.grpc.v1.events.AppendOperation;
import io.mubel.api.grpc.v1.events.Deadline;
import io.mubel.api.grpc.v1.events.EventDataInput;
import io.mubel.api.grpc.v1.events.ExecuteRequest;
import io.mubel.server.spi.Fixtures;
import io.mubel.server.spi.TestEventStore;
import io.mubel.server.spi.eventstore.ExecuteRequestHandler;
import io.mubel.server.spi.queue.BatchSendRequest;
import io.mubel.server.spi.queue.MessageQueueService;
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

import static io.mubel.server.spi.execute.TestOperations.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BatchingExecuteRequestHandlerTest {

    public static final String ESID = "esid";

    TestEventStore eventStore = new TestEventStore();

    @Mock
    MessageQueueService messageQueueService;

    ExecuteRequestHandler handler;

    @BeforeEach
    void setUp() {
        handler = new BatchingExecuteRequestHandler(ESID, eventStore, messageQueueService);
    }

    @AfterEach
    void tearDown() {
        handler.stop();
    }

    @Test
    void singe_append_operation() {
        var request = ExecuteRequest.newBuilder()
                .setRequestId(ESID)
                .addOperation(appendOperation())
                .build();

        assertThat(handler.handle(request))
                .succeedsWithin(Duration.ofMillis(10));

        assertThat(eventStore.firstAppendOperation()).isEqualTo(request.getOperation(0).getAppend());
    }

    @Test
    void all_operations() {
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

        assertThat(eventStore.firstAppendOperation()).isEqualTo(request.getOperation(0).getAppend());
        verify(messageQueueService, times(2)).send(any(BatchSendRequest.class));
        verify(messageQueueService).delete(List.of(cancelId));
    }

    @Test
    void queued_operations_are_joined_in_single_request() {
        var eventId1 = Fixtures.uuidString();
        var deadlineIdTargetId1 = Fixtures.uuidString();
        var r1 = ExecuteRequest.newBuilder()
                .setRequestId(ESID)
                .addOperation(appendOperation(eventId1))
                .addOperation(scheduleDeadlineOperation(deadlineIdTargetId1))
                .build();
        var eventId2 = Fixtures.uuidString();
        var deadlineIdTargetId2 = Fixtures.uuidString();
        var r2 = ExecuteRequest.newBuilder()
                .setRequestId(ESID)
                .addOperation(appendOperation(eventId2))
                .addOperation(scheduleDeadlineOperation(deadlineIdTargetId2))
                .build();
        var r1Future = handler.handle(r1);
        var r2Future = handler.handle(r2);
        assertThat(r1Future).succeedsWithin(Duration.ofMillis(100));
        assertThat(r2Future).succeedsWithin(Duration.ofMillis(100));

        AppendOperation aop = eventStore.lastAppendOperation();
        assertThat(aop.getEventList())
                .as("all events are appended in same operation")
                .hasSize(2)
                .map(EventDataInput::getId)
                .as("events are appended in queue order")
                .containsExactly(eventId1, eventId2);

        var deadlineCaptor = ArgumentCaptor.forClass(BatchSendRequest.class);
        verify(messageQueueService, times(1)).send(deadlineCaptor.capture());
        assertThat(deadlineCaptor.getValue().entries())
                .as("all deadlines are sent in same batch")
                .hasSize(2)
                .map(BatchSendRequest.BatchEntry::payload)
                .map(Deadline::parseFrom)
                .map(dl -> dl.getTargetEntity().getId())
                .as("deadlines are sent in queue order")
                .containsExactly(deadlineIdTargetId1, deadlineIdTargetId2);
    }

    @Test
    void multiple_scheduled_event_ops_in_same_request_are_joined_in_a_single_batch_request() {
        var eventId1 = Fixtures.uuidString();
        var eventId2 = Fixtures.uuidString();
        var r1 = ExecuteRequest.newBuilder()
                .setRequestId(ESID)
                .addOperation(scheduleEventOperation(eventId1))
                .addOperation(scheduleEventOperation(eventId2))
                .build();
        var r1Future = handler.handle(r1);

        assertThat(r1Future).succeedsWithin(Duration.ofMillis(100));

        var scheduleCaptor = ArgumentCaptor.forClass(BatchSendRequest.class);
        verify(messageQueueService, times(1)).send(scheduleCaptor.capture());
        assertThat(scheduleCaptor.getValue().entries())
                .as("all schedules are sent in same batch")
                .hasSize(2)
                .map(BatchSendRequest.BatchEntry::payload)
                .map(EventDataInput::parseFrom)
                .map(EventDataInput::getId)
                .as("schedules are sent in queue order")
                .containsExactly(eventId1, eventId2);
    }

    @Test
    void multiple_cancel_ops_are_joined_into_a_single_operation() throws Exception {
        var cancelId1 = Fixtures.uuid();
        var cancelId2 = Fixtures.uuid();
        var r1 = ExecuteRequest.newBuilder()
                .setRequestId(ESID)
                .addOperation(cancelScheduledOperation(cancelId1))
                .addOperation(cancelScheduledOperation(cancelId2))
                .build();
        handler.handle(r1).get();

        verify(messageQueueService).delete(List.of(cancelId1, cancelId2));
    }

}