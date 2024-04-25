package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.Deadline;
import io.mubel.api.grpc.v1.events.EntityReference;

import static io.mubel.server.api.grpc.validation.CommonConstraints.eventId;
import static io.mubel.server.api.grpc.validation.CommonConstraints.eventType;

public final class DeadlineValidator {

    private DeadlineValidator() {
    }

    public static final Validator<EntityReference> ENTITY_REFERENCE_VALIDATOR = ValidatorBuilder.of(EntityReference.class)
            .constraint(EntityReference::getId, "id", eventId())
            ._string(EntityReference::getType, "type", eventType())
            .build();

    public static final Validator<Deadline> VALIDATOR = ValidatorBuilder.of(Deadline.class)
            .constraint(Deadline::getType, "type", type -> type.pattern(CommonConstraints.EVENT_TYPE_PTRN))
            .nest(Deadline::getTargetEntity, "targetEntity", ENTITY_REFERENCE_VALIDATOR)
            .build();

}
