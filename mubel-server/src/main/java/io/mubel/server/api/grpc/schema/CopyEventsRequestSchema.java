package io.mubel.server.api.grpc.schema;

import io.mubel.api.grpc.CopyEventsRequest;
import io.mubel.schema.ObjectSchema;
import io.mubel.schema.StringSchema;
import io.mubel.schema.ValidationContext;

public class CopyEventsRequestSchema extends ObjectSchema<CopyEventsRequest> {

    private final static StringSchema esidSchema = CommonSchema.esidSchema.required();

    public static CopyEventsRequestSchema get() {
        return new CopyEventsRequestSchema();
    }

    @Override
    protected CopyEventsRequest doValidate(ValidationContext<CopyEventsRequest> ctx) {
        var r = CopyEventsRequest.newBuilder(ctx.currentValue());
        r.setSourceEsid(esidSchema.validate(ctx.push(r.getSourceEsid())));
        r.setTargetEsid(esidSchema.validate(ctx.push(r.getTargetEsid())));
        return r.build();
    }
}
