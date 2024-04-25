package io.mubel.server.api.grpc.validation;


import am.ik.yavi.core.ConstraintViolations;
import io.mubel.api.grpc.v1.events.ExecuteRequest;
import io.mubel.api.grpc.v1.events.GetEventsRequest;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import io.mubel.api.grpc.v1.server.DropEventStoreRequest;
import io.mubel.api.grpc.v1.server.ProvisionEventStoreRequest;
import io.mubel.server.ValidationException;

public final class Validators {

    private Validators() {
    }

    public static ExecuteRequest validate(ExecuteRequest input) {
        handleResult(ExecuteRequestValidator.validate(input));
        return input;
    }

    public static GetEventsRequest validate(GetEventsRequest input) {
        handleResult(GetEventsRequestValidator.validate(input));
        return input;
    }

    public static ProvisionEventStoreRequest validate(ProvisionEventStoreRequest input) {
        var validator = ProvisionEventStoreRequestValidator.VALIDATOR;
        handleResult(validator.validate(input));
        return input;
    }

    public static DropEventStoreRequest validate(DropEventStoreRequest request) {
        handleResult(DropEventStoreRequestValidator.VALIDATOR.validate(request));
        return request;
    }


    public static SubscribeRequest validate(SubscribeRequest request) {
        handleResult(SubscribeRequestValidator.validate(request));
        return request;
    }

    private static void handleResult(ConstraintViolations violations) {
        if (!violations.isValid()) {
            throw new ValidationException(violations);
        }
    }

}
