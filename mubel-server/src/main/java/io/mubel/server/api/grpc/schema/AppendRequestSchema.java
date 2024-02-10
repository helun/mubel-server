package io.mubel.server.api.grpc.schema;

import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.EventDataInput;
import io.mubel.schema.ListSchema;
import io.mubel.schema.ObjectSchema;
import io.mubel.schema.StringSchema;
import io.mubel.schema.ValidationContext;

public class AppendRequestSchema extends ObjectSchema<AppendRequest> {

    private final static StringSchema esidSchema = CommonSchema.esidSchema.required();
    private final static ListSchema<EventDataInput> eventsSchema = ListSchema.builder("events")
            .elements(EventDataInputSchema.get())
            .required();

    public static AppendRequestSchema get() {
        return new AppendRequestSchema();
    }

    protected AppendRequest doValidate(final ValidationContext<AppendRequest> ctx) {
        var v = AppendRequest.newBuilder(ctx.currentValue());
        v.setEsid(esidSchema.validate(ctx.push(v.getEsid())));
        eventsSchema.validate(ctx.push(v.getEventList()));
        return v.build();
    }
}
