package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.ConstraintViolations;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.events.ExecuteRequest;
import io.mubel.api.grpc.v1.events.Operation;

import static io.mubel.server.api.grpc.validation.CommonConstraints.esid;

public class ExecuteRequestValidator {

    private static final Validator<Operation> OPERATION_VALIDATOR = OperationValidator.OPERATION_VALIDATOR;

    private static final Validator<ExecuteRequest> VALIDATOR = ValidatorBuilder.<ExecuteRequest>of()
            .constraint(ExecuteRequest::getEsid, "esid", esid())
            .forEach(ExecuteRequest::getOperationList, "operation", OPERATION_VALIDATOR)
            .build();

    public static ConstraintViolations validate(ExecuteRequest request) {
        return VALIDATOR.validate(request);
    }

}
