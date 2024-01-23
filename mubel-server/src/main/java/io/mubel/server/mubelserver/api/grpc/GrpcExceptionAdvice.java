package io.mubel.server.mubelserver.api.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.mubel.server.spi.exceptions.BadRequestException;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import net.devh.boot.grpc.server.advice.GrpcAdvice;
import net.devh.boot.grpc.server.advice.GrpcExceptionHandler;

@GrpcAdvice
public class GrpcExceptionAdvice {


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
