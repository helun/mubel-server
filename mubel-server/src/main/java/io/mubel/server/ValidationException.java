package io.mubel.server;

import io.grpc.Status;
import io.mubel.api.grpc.v1.common.ProblemDetail;
import io.mubel.schema.ValidationError;
import io.mubel.server.spi.exceptions.BadRequestException;

import java.util.List;
import java.util.stream.Collectors;

public class ValidationException extends BadRequestException {

    private final ProblemDetail problemDetail;

    public ValidationException(List<ValidationError> errors) {
        super("Request validation failed");
        this.problemDetail = createProblemDetail(errors);
    }

    public ProblemDetail problemDetail() {
        return problemDetail;
    }

    private static ProblemDetail createProblemDetail(List<ValidationError> errors) {
        var first = errors.getFirst();
        final String title;
        if (first != null) {
            title = first.message();
        } else {
            title = "Request validation failed";
        }
        var detail = errors.stream()
                .map(ValidationError::message)
                .collect(Collectors.joining(",\n", "Errors:\n", ""));

        return ProblemDetail.newBuilder()
                .setType("https://mubel.io/problems/validation-failed")
                .setTitle(title)
                .setStatus(Status.Code.INVALID_ARGUMENT.value())
                .setDetail(detail)
                .build();
    }
}
