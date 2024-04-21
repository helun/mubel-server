package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.constraint.CharSequenceConstraint;
import am.ik.yavi.core.ConstraintViolations;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.ExecuteRequest;

public class ExecuteRequestValidator {

    private static final Validator<ExecuteRequest> VALIDATOR = ValidatorBuilder.<ExecuteRequest>of()
            .constraint(ExecuteRequest::getEsid, "esid", CharSequenceConstraint::notBlank)
            .build();

    public static ConstraintViolations validate(ExecuteRequest request) {
        return VALIDATOR.validate(request);
    }

}
