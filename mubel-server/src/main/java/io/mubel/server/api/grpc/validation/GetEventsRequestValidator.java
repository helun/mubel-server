package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.ConstraintViolations;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.GetEventsRequest;

import static io.mubel.server.api.grpc.validation.CommonConstraints.esid;
import static io.mubel.server.api.grpc.validation.EventSelectorValidator.EVENT_SELECTOR_VALIDATOR;

public final class GetEventsRequestValidator {

    private GetEventsRequestValidator() {
    }

    private static final Validator<GetEventsRequest> VALIDATOR = ValidatorBuilder.<GetEventsRequest>of()
            .constraint(GetEventsRequest::getEsid, "esid", esid())
            .nest(GetEventsRequest::getSelector, "selector", EVENT_SELECTOR_VALIDATOR)
            .constraintOnCondition((op, group) -> op.hasSize(), op -> op.constraint(GetEventsRequest::getSize, "size", size -> size.greaterThanOrEqual(1)))
            .build();

    public static ConstraintViolations validate(GetEventsRequest input) {
        return VALIDATOR.validate(input);
    }
}
