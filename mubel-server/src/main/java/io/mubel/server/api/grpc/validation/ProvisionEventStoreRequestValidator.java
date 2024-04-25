package io.mubel.server.api.grpc.validation;

import am.ik.yavi.builder.ValidatorBuilder;
import am.ik.yavi.core.ConstraintViolations;
import am.ik.yavi.core.Validator;
import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.api.grpc.v1.server.ProvisionEventStoreRequest;

import static io.mubel.server.api.grpc.validation.CommonConstraints.esid;
import static io.mubel.server.api.grpc.validation.CommonConstraints.safeString;

public final class ProvisionEventStoreRequestValidator {

    private ProvisionEventStoreRequestValidator() {
    }

    public static final Validator<ProvisionEventStoreRequest> VALIDATOR = ValidatorBuilder.<ProvisionEventStoreRequest>of()
            .constraint(ProvisionEventStoreRequest::getEsid, "esid", esid())
            .constraint(ProvisionEventStoreRequest::getDataFormat, "dataFormat", c -> c.oneOf(DataFormat.JSON, DataFormat.PROTO, DataFormat.OTHER))
            .constraint(ProvisionEventStoreRequest::getStorageBackendName, "storageBackendName", safeString())
            .build();

    public static ConstraintViolations validate(ProvisionEventStoreRequest request) {
        return VALIDATOR.validate(request);
    }
}
