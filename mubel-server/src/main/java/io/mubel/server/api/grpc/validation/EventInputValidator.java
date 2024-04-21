package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.EventDataInput;

public final class EventInputValidator {

    public static final Validator<EventDataInput> VALIDATOR = ValidatorBuilder.<EventDataInput>of()
            .constraint(EventDataInput::getId, "eventId", id -> id.notBlank().pattern(CommonConstraints.UUID_REGEX))
            .constraint(EventDataInput::getStreamId, "streamId", id -> id.notBlank().pattern(CommonConstraints.UUID_REGEX))
            .constraint(EventDataInput::getType, "type", type -> type.notBlank().pattern(CommonConstraints.EVENT_TYPE_PTRN))
            .constraint(EventDataInput::getRevision, "revision", rev -> rev.greaterThanOrEqual(0))
            .build();

    private EventInputValidator() {
        EventDataInput e = null;

    }


}
