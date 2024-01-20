package io.mubel.provider.jdbc.eventstore;

public interface ErrorMapper {
    RuntimeException map(Exception e);

}
