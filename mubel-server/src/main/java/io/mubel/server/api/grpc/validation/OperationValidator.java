package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.constraint.CharSequenceConstraint;
import am.ik.yavi.constraint.base.ContainerConstraintBase;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.*;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

import static io.mubel.server.api.grpc.validation.CommonConstraints.eventId;

public class OperationValidator {

    private static final Set<Operation.OperationCase> VALID_OPERATION_CASES = EnumSet.of(
            Operation.OperationCase.APPEND,
            Operation.OperationCase.SCHEDULEEVENT,
            Operation.OperationCase.SCHEDULEDEADLINE,
            Operation.OperationCase.CANCEL
    );

    public static final Validator<AppendOperation> APPEND_OPERATION_VALIDATOR = ValidatorBuilder.<AppendOperation>of()
            .constraint(AppendOperation::getEventList, "eventList", ContainerConstraintBase::notEmpty)
            .forEach(AppendOperation::getEventList, "eventList", EventInputValidator.VALIDATOR)
            .build();

    public static final Validator<ScheduleEventOperation> SCHEDULE_EVENT_OPERATION_VALIDATOR = ValidatorBuilder.<ScheduleEventOperation>of()
            .nest(ScheduleEventOperation::getEvent, "event", EventInputValidator.VALIDATOR)
            .constraint(ScheduleEventOperation::getPublishTime, "publishTime", time -> time.greaterThanOrEqual(0L))
            .build();

    public static final Validator<ScheduleDeadlineOperation> SCHEDULE_DEADLINE_OPERATION_VALIDATOR = ValidatorBuilder.<ScheduleDeadlineOperation>of()
            .constraint(ScheduleDeadlineOperation::getId, "id", CharSequenceConstraint::uuid)
            .nest(ScheduleDeadlineOperation::getDeadline, "deadline", DeadlineValidator.VALIDATOR)
            .constraint(ScheduleDeadlineOperation::getPublishTime, "publishTime", time -> time.greaterThanOrEqual(0L))
            .build();

    private static final Validator<CancelScheduledOperation> CANCEL_SCHEDULED_OPERATION_VALIDATOR = ValidatorBuilder.of(CancelScheduledOperation.class)
            .constraint(CancelScheduledOperation::getEventIdList, "eventIdList", ContainerConstraintBase::notEmpty)
            .forEach(myLambda(), "eventIdList", s -> s._string(it -> it, "", eventId()))
            .build();

    private static ValidatorBuilder.ToCollection<CancelScheduledOperation, Collection<String>, String> myLambda() {
        return CancelScheduledOperation::getEventIdList;
    }

    public static final Validator<Operation> OPERATION_VALIDATOR = ValidatorBuilder.<Operation>of()
            .constraint(Operation::getOperationCase, "operation", op -> op.oneOf(VALID_OPERATION_CASES))
            .constraintOnCondition((op, group) -> op.hasAppend(), op -> op.nest(Operation::getAppend, "append", APPEND_OPERATION_VALIDATOR))
            .constraintOnCondition((op, group) -> op.hasScheduleEvent(), op -> op.nest(Operation::getScheduleEvent, "scheduleEvent", SCHEDULE_EVENT_OPERATION_VALIDATOR))
            .constraintOnCondition((op, group) -> op.hasScheduleDeadline(), op -> op.nest(Operation::getScheduleDeadline, "scheduleDeadline", SCHEDULE_DEADLINE_OPERATION_VALIDATOR))
            .constraintOnCondition((op, group) -> op.hasCancel(), op -> op.nest(Operation::getCancel, "cancel", CANCEL_SCHEDULED_OPERATION_VALIDATOR))
            .build();
}
