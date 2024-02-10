package io.mubel.server.api.grpc;

import io.mubel.api.grpc.*;
import io.mubel.schema.ObjectSchema;
import io.mubel.schema.ValidationContext;
import io.mubel.server.ValidationException;
import io.mubel.server.api.grpc.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class Validator {

    private final static Logger LOG = LoggerFactory.getLogger(Validator.class);
    private final static GetEventsSchema GET_EVENTS_SCHEMA = GetEventsSchema.get();
    private final static AppendRequestSchema APPEND_REQUEST_SCHEMA = AppendRequestSchema.get();
    private final static CopyEventsRequestSchema COPY_EVENTS_REQUEST_SCHEMA = CopyEventsRequestSchema.get();
    private final static ProvisionRequestSchema PROVISION_REQUEST_SCHEMA = ProvisionRequestSchema.get();
    private final static DropEventStoreRequestSchema DROP_EVENT_STORE_REQUEST_SCHEMA = DropEventStoreRequestSchema.get();

    private final static Map<Class<?>, ObjectSchema<?>> SCHEMAS = Map.of(
            GetEventsRequest.class, GET_EVENTS_SCHEMA,
            AppendRequest.class, APPEND_REQUEST_SCHEMA,
            CopyEventsRequest.class, COPY_EVENTS_REQUEST_SCHEMA,
            ProvisionEventStoreRequest.class, PROVISION_REQUEST_SCHEMA,
            DropEventStoreRequest.class, DROP_EVENT_STORE_REQUEST_SCHEMA
    );

    private Validator() {
    }

    public static <T> T validate(T input) {
        var schema = resolveSchema(input);
        var ctx = ValidationContext.create(input);
        var validated = schema.validate(ctx);
        if (ctx.hasErrors()) {
            LOG.debug("Validation of {}, failed: {}", input.getClass().getSimpleName(), ctx.errors());
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
