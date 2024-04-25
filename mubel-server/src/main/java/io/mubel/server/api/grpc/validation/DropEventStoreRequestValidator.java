package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.ConstraintViolations;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.server.DropEventStoreRequest;

import static io.mubel.server.api.grpc.validation.CommonConstraints.esid;

public class DropEventStoreRequestValidator {

    public static final Validator<DropEventStoreRequest> VALIDATOR = ValidatorBuilder.<DropEventStoreRequest>of()
            .constraint(DropEventStoreRequest::getEsid, "esid", esid())
            .build();

    public static ConstraintViolations validate(DropEventStoreRequest request) {
        return VALIDATOR.validate(request);
    }

}
