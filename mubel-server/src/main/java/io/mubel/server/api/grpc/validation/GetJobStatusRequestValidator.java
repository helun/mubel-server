package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.ConstraintViolations;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.server.GetJobStatusRequest;

public class GetJobStatusRequestValidator {

    private static final Validator<GetJobStatusRequest> VALIDATOR = ValidatorBuilder.<GetJobStatusRequest>of()
            .constraint(GetJobStatusRequest::getJobId, "jobId", c -> c.notBlank().uuid())
            .build();

    public static ConstraintViolations validate(GetJobStatusRequest input) {
        return VALIDATOR.validate(input);
    }

}
