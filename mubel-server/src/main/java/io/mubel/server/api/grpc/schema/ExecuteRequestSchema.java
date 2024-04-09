package io.mubel.server.api.grpc.schema;

import io.mubel.api.grpc.v1.events.EventDataInput;
import io.mubel.api.grpc.v1.events.ExecuteRequest;
import io.mubel.schema.ListSchema;
import io.mubel.schema.ObjectSchema;
import io.mubel.schema.StringSchema;
import io.mubel.schema.ValidationContext;

public class ExecuteRequestSchema extends ObjectSchema<ExecuteRequest> {

    private final static StringSchema esidSchema = CommonSchema.esidSchema.required();
    private final static ListSchema<EventDataInput> operationsSchema = ListSchema.<EventDataInput>builder("events")
            .elements(EventDataInputSchema.get())
            .required();

    public static ExecuteRequestSchema get() {
        return new ExecuteRequestSchema();
    }

    protected ExecuteRequest doValidate(final ValidationContext<ExecuteRequest> ctx) {
        var v = ExecuteRequest.newBuilder(ctx.currentValue());
        v.setEsid(esidSchema.validate(ctx.push(v.getEsid())));
        //operationsSchema.validate(ctx.push(v.getOperationList()));
        return v.build();
    }
}
