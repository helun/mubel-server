package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.Deadline;
import io.mubel.api.grpc.v1.events.EntityReference;

public final class DeadlineValidator {

    private DeadlineValidator() {
    }

    public static final Validator<EntityReference> ENTITY_REFERENCE_VALIDATOR = ValidatorBuilder.of(EntityReference.class)
            .constraint(EntityReference::getId, "id", id -> id.notBlank().pattern(CommonConstraints.UUID_REGEX))
            .constraint(EntityReference::getType, "type", type -> type.notBlank().pattern(CommonConstraints.EVENT_TYPE_PTRN))
            .build();

    public static final Validator<Deadline> VALIDATOR = ValidatorBuilder.of(Deadline.class)
            .constraint(Deadline::getType, "type", type -> type.pattern(CommonConstraints.EVENT_TYPE_PTRN))
            .nest(Deadline::getTargetEntity, "targetEntity", ENTITY_REFERENCE_VALIDATOR)
            .build();

}
