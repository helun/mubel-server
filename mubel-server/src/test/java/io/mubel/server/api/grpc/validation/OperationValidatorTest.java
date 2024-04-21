package io.mubel.server.api.grpc.validation;

import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.*;
import io.mubel.server.Fixtures;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperationValidatorTest {

    Validator<Operation> validator = OperationValidator.OPERATION_VALIDATOR;

    @Test
    void no_operation_set_is_not_valid() {
        var op = Operation.newBuilder()
                .build();
        assertInvalid(op, "no operation set should not validate");
    }

    @Test
    void append_operation_is_valid() {
        var op = Operation.newBuilder()
                .setAppend(AppendOperation.newBuilder()
                        .addEvent(Fixtures.eventInput(0))
                )
                .build();
        assertValid(op, "append operation should validate");
    }

    @Test
    void invalid_event_in_append_operation_is_not_valid() {
        var op = Operation.newBuilder()
                .setAppend(AppendOperation.newBuilder()
                        .addEvent(Fixtures.eventInput(-99))
                )
                .build();
        assertInvalid(op, "invalid event input should not validate");
    }

    @Test
    void schedule_event_operation_is_valid() {
        var op = Operation.newBuilder()
                .setScheduleEvent(ScheduleEventOperation.newBuilder()
                        .setEvent(Fixtures.eventInput(0))
                        .setPublishTime(System.currentTimeMillis())
                )
                .build();
        assertValid(op, "schedule event operation should validate");
    }

    @Test
    void invalid_event_in_schedule_event_operation_is_not_valid() {
        var op = Operation.newBuilder()
                .setScheduleEvent(ScheduleEventOperation.newBuilder()
                        .setEvent(Fixtures.eventInput(-99))
                        .setPublishTime(System.currentTimeMillis())
                )
                .build();
        assertInvalid(op, "schedule event operation should validate");
    }

    @Test
    void schedule_deadline_operation_is_valid() {
        var op = Operation.newBuilder()
                .setScheduleDeadline(ScheduleDeadlineOperation.newBuilder()
                        .setId(Fixtures.uuid())
                        .setPublishTime(System.currentTimeMillis())
                        .setDeadline(Fixtures.deadline())
                )
                .build();
        assertValid(op, "schedule deadline operation should validate");
    }

    @Test
    void invalid_deadline_schedule_deadline_operation_is_valid() {
        var op = Operation.newBuilder()
                .setScheduleDeadline(ScheduleDeadlineOperation.newBuilder()
                        .setId(Fixtures.uuid())
                        .setPublishTime(System.currentTimeMillis())
                        .setDeadline(Fixtures.deadline()
                                .setTargetEntity(EntityReference.newBuilder()
                                        // missing id
                                        .setType("a-type")
                                        .build())
                        )
                        .setPublishTime(System.currentTimeMillis())
                )
                .build();
        assertInvalid(op, "schedule deadline operation should validate");
    }

    private void assertInvalid(Operation op, String description) {
        assertThat(validator.validate(op))
                .as(description)
                .isNotEmpty();
    }

    private void assertValid(Operation op, String description) {
        assertThat(validator.validate(op))
                .as(description)
                .isEmpty();
    }

}