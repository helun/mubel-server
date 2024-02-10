package io.mubel.server.api.grpc;

import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.CopyEventsRequest;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.schema.ObjectSchema;
import io.mubel.schema.ValidationContext;
import io.mubel.server.ValidationException;
import io.mubel.server.api.grpc.schema.AppendRequestSchema;
import io.mubel.server.api.grpc.schema.CopyEventsRequestSchema;
import io.mubel.server.api.grpc.schema.GetEventsSchema;
import io.mubel.server.api.grpc.schema.ProvisionRequestSchema;

import java.util.Map;

public final class Validator {

    private final static GetEventsSchema GET_EVENTS_SCHEMA = GetEventsSchema.get();
    private final static AppendRequestSchema APPEND_REQUEST_SCHEMA = AppendRequestSchema.get();
    private final static CopyEventsRequestSchema COPY_EVENTS_REQUEST_SCHEMA = CopyEventsRequestSchema.get();
    private final static ProvisionRequestSchema PROVISION_REQUEST_SCHEMA = io.mubel.server.api.grpc.schema.ProvisionRequestSchema.get();

    private final static Map<Class<?>, ObjectSchema<?>> SCHEMAS = Map.of(
            GetEventsRequest.class, GET_EVENTS_SCHEMA,
            AppendRequest.class, APPEND_REQUEST_SCHEMA,
            CopyEventsRequest.class, COPY_EVENTS_REQUEST_SCHEMA,
            ProvisionEventStoreRequest.class, PROVISION_REQUEST_SCHEMA
    );

    private Validator() {
    }

    public static <T> T validate(T input) {
        var schema = resolveSchema(input);
        var ctx = ValidationContext.create(input);
        var validated = schema.validate(ctx);
        if (ctx.hasErrors()) {
            throw new ValidationException(ctx.errors());
        }
        return validated;
    }

    @SuppressWarnings("unchecked")
    private static <T> ObjectSchema<T> resolveSchema(T input) {
        var schema = SCHEMAS.get(input.getClass());
        if (schema == null) {
            throw new IllegalArgumentException("No schema found for " + input.getClass());
        }
        return (ObjectSchema<T>) schema;
    }
}
