package io.mubel.server;

import am.ik.yavi.core.ConstraintViolation;
import am.ik.yavi.core.ConstraintViolations;
import io.grpc.Status;
import io.mubel.api.grpc.v1.common.ProblemDetail;
import io.mubel.server.spi.exceptions.BadRequestException;

import java.util.stream.Collectors;

public class ValidationException extends BadRequestException {

    private final ProblemDetail problemDetail;

    public ValidationException(ConstraintViolations violations) {
        super("Request validation failed");
        this.problemDetail = createProblemDetail(violations);
    }

    public ProblemDetail problemDetail() {
        return problemDetail;
    }

    private static ProblemDetail createProblemDetail(ConstraintViolations violations) {
        var first = violations.getFirst();
        final String title;
        if (first != null) {
            title = first.message();
        } else {
            title = "Request validation failed";
        }
        var detail = violations.stream()
                .map(ConstraintViolation::message)
                .collect(Collectors.joining(",\n", "Errors:\n", ""));

        return ProblemDetail.newBuilder()
                .setType("https://mubel.io/problems/validation-failed")
                .setTitle(title)
                .setStatus(Status.Code.INVALID_ARGUMENT.value())
                .setDetail(detail)
                .build();
    }
}
