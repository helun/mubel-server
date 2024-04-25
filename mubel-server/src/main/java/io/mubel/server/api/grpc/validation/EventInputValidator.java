package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.EventDataInput;

import static io.mubel.server.api.grpc.validation.CommonConstraints.*;

public final class EventInputValidator {

    public static final Validator<EventDataInput> VALIDATOR = ValidatorBuilder.<EventDataInput>of()
            ._string(EventDataInput::getId, "eventId", eventId())
            ._string(EventDataInput::getStreamId, "streamId", streamId())
            ._string(EventDataInput::getType, "type", eventType())
            .constraint(EventDataInput::getRevision, "revision", rev -> rev.greaterThanOrEqual(0))
            .build();

    private EventInputValidator() {
        EventDataInput e = null;

    }


}
