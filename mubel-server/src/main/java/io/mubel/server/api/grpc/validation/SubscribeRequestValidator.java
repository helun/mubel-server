package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.ConstraintViolations;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.SubscribeRequest;

import static io.mubel.server.api.grpc.validation.CommonConstraints.esid;

public class SubscribeRequestValidator {

    public static final Validator<SubscribeRequest> SUBSRIBE_REQUEST_VALIDATOR = ValidatorBuilder.<SubscribeRequest>of()
            ._string(SubscribeRequest::getEsid, "esid", esid())
            .constraint(SubscribeRequest::getTimeout, "timeout", timeout -> timeout.greaterThanOrEqual(0).lessThanOrEqual(20))
            .nest(SubscribeRequest::getSelector, "selector", EventSelectorValidator.EVENT_SELECTOR_VALIDATOR)
            .constraintOnCondition((r, group) -> r.hasMaxEvents(), op -> op.constraint(SubscribeRequest::getMaxEvents, "size", size -> size.greaterThanOrEqual(1)))
            .build();

    public static ConstraintViolations validate(SubscribeRequest request) {
        return SUBSRIBE_REQUEST_VALIDATOR.validate(request);
    }
}
