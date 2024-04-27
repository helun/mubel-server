package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.ConstraintViolations;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.DeadlineSubscribeRequest;

public class DeadlineSubscribeRequestValidator {

    private static final Validator<DeadlineSubscribeRequest> VALIDATOR = ValidatorBuilder.<DeadlineSubscribeRequest>of()
            .constraint(DeadlineSubscribeRequest::getEsid, "esid", CommonConstraints.esid())
            .constraint(DeadlineSubscribeRequest::getTimeout, "timeout", timeout -> timeout.greaterThanOrEqual(0).lessThanOrEqual(20))
            .constraintOnCondition((r, group) -> r.hasMaxEvents(), op -> op.constraint(DeadlineSubscribeRequest::getMaxEvents, "maxEvents", size -> size.greaterThanOrEqual(1)))
            .build();

    public static ConstraintViolations validate(DeadlineSubscribeRequest input) {
        return VALIDATOR.validate(input);
    }

}
