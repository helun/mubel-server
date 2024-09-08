package io.mubel.server.spi.execute;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.server.spi.Fixtures;

import java.util.UUID;

public final class TestOperations {

    private TestOperations() {
    }

    static Operation.Builder appendOperation() {
        return appendOperation(UUID.randomUUID().toString());
    }

    static Operation.Builder appendOperation(String eventId) {
        return Operation.newBuilder()
                .setAppend(AppendOperation.newBuilder()
                        .addEvent(EventDataInput.newBuilder()
                                .setId(eventId)
                                .setType("test-type")
                                .setStreamId(UUID.randomUUID().toString())
                        )
                );
    }

    static Operation.Builder cancelScheduledOperation(UUID cancelId) {
        return Operation.newBuilder()
                .setCancel(CancelScheduledOperation.newBuilder()
                        .addEventId(cancelId.toString())
                );
    }

    static Operation.Builder scheduleDeadlineOperation() {
        return scheduleDeadlineOperation(UUID.randomUUID().toString());
    }

    static Operation.Builder scheduleDeadlineOperation(String targetId) {
        return Operation.newBuilder()
                .setScheduleDeadline(ScheduleDeadlineOperation.newBuilder()
                        .setId(Fixtures.uuidString())
                        .setDeadline(Deadline.newBuilder()
                                .setType("test-dl")
                                .setTargetEntity(EntityReference.newBuilder()
                                        .setId(targetId)
                                        .setType("test-entity")
                                )
                        )
                        .setPublishTime(System.currentTimeMillis() + 1000)
                );
    }

    static Operation.Builder scheduleEventOperation() {
        return scheduleEventOperation(Fixtures.uuidString());
    }

    static Operation.Builder scheduleEventOperation(String eventId) {
        return Operation.newBuilder()
                .setScheduleEvent(ScheduleEventOperation.newBuilder()
                        .setEvent(EventDataInput.newBuilder()
                                .setId(eventId)
                                .setType("test-type")
                                .setStreamId(Fixtures.uuidString())
                        )
                        .setPublishTime(System.currentTimeMillis() + 1000)
                );
    }
}
