package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.ConstraintViolations;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.server.GetEventStoreSummaryRequest;

public class GetEventStoreSummaryRequestValidator {

    private static final Validator<GetEventStoreSummaryRequest> VALIDATOR = ValidatorBuilder.<GetEventStoreSummaryRequest>of()
            .constraint(GetEventStoreSummaryRequest::getEsid, "esid", CommonConstraints.esid())
            .build();

    public static ConstraintViolations validate(GetEventStoreSummaryRequest input) {
        return VALIDATOR.validate(input);
    }

}
