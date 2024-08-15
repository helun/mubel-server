package io.mubel.server.api.grpc;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.mubel.api.grpc.v1.common.ProblemDetail;
import io.mubel.server.ValidationException;
import io.mubel.server.spi.exceptions.BadRequestException;
import io.mubel.server.spi.exceptions.PermissionDeniedException;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcAdvice
public class GrpcExceptionAdvice {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcExceptionAdvice.class);

    @GrpcExceptionHandler
    public StatusRuntimeException handle(ValidationException e) {
        LOG.warn("Validation error {}", e.getMessage());
        Status status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
        Metadata metadata = metadataWithProblemDetail(e.problemDetail());
        return status.asRuntimeException(metadata);
    }

    private static Metadata metadataWithProblemDetail(ProblemDetail pd) {
        Metadata metadata = new Metadata();
        Metadata.Key<ProblemDetail> problemKey = ProtoUtils.keyForProto(pd);
        metadata.put(problemKey, pd);
        return metadata;
    }

    @GrpcExceptionHandler
    public StatusRuntimeException handle(BadRequestException e) {
        LOG.warn("Bad request {}", e.getMessage());
        Status status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
        return status.asRuntimeException();
    }

    @GrpcExceptionHandler
    public StatusRuntimeException handle(ResourceNotFoundException e) {
        LOG.warn("Resource not found", e);
        Status status = Status.NOT_FOUND.withDescription(e.getMessage());
        return status.asRuntimeException();
    }

    @GrpcExceptionHandler
    public StatusRuntimeException handle(PermissionDeniedException e) {
        LOG.warn("Permission denied", e);
        Status status = Status.PERMISSION_DENIED.withDescription(e.getMessage());
        return status.asRuntimeException();
    }

    @GrpcExceptionHandler
    public StatusRuntimeException handle(Throwable e) {
        LOG.error("Error in gRPC service", e);
        Status status = Status.INTERNAL.withDescription(e.getMessage());
        return status.asRuntimeException();
    }

    public StatusRuntimeException handleException(Throwable e) {
        return switch (e) {
            case ValidationException ve -> handle(ve);
            case ResourceNotFoundException rnfe -> handle(rnfe);
            case PermissionDeniedException pde -> handle(pde);
            case BadRequestException bre -> handle(bre);
            case null, default -> handle(e);
        };
    }
}
