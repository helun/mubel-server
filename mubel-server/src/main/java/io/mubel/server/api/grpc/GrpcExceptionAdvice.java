package io.mubel.server.api.grpc;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.mubel.api.grpc.v1.common.ProblemDetail;
import io.mubel.server.ValidationException;
import io.mubel.server.spi.exceptions.BadRequestException;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;

@GrpcAdvice
public class GrpcExceptionAdvice {


    @GrpcExceptionHandler
    public StatusRuntimeException handle(ValidationException e) {
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
        Status status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
        return status.asRuntimeException();
    }

    @GrpcExceptionHandler
    public StatusRuntimeException handle(ResourceNotFoundException e) {
        Status status = Status.NOT_FOUND.withDescription(e.getMessage());
        return status.asRuntimeException();
    }

    @GrpcExceptionHandler
    public StatusRuntimeException handle(Exception e) {
        Status status = Status.INTERNAL.withDescription(e.getMessage());
        return status.asRuntimeException();
    }
}
