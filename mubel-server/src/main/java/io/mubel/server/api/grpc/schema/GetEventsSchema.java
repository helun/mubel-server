package io.mubel.server.api.grpc.schema;

import io.mubel.api.grpc.v1.events.GetEventsRequest;
import io.mubel.schema.LongSchema;
import io.mubel.schema.ObjectSchema;
import io.mubel.schema.StringSchema;
import io.mubel.schema.ValidationContext;

public class GetEventsSchema extends ObjectSchema<GetEventsRequest> {

    private final static StringSchema esidSchema = CommonSchema.esidSchema.required();
    private final static StringSchema streamId = CommonSchema.streamIdSchema.build();
    private final static LongSchema sequenceNoSchema = LongSchema.builder("fromSequenceNo")
            .defaultValue(0L)
            .minInclusive(0L)
            .build();

    public static GetEventsSchema get() {
        return new GetEventsSchema();
    }

    protected GetEventsRequest doValidate(final ValidationContext<GetEventsRequest> ctx) {
        var v = GetEventsRequest.newBuilder(ctx.currentValue());
        v.setEsid(esidSchema.validate(ctx.push(v.getEsid())));
        //v.setStreamId(streamId.validate(ctx.push(v.getStreamId())));
        //v.setFromVersion(CommonSchema.fromVersionSchema.validate(ctx.push(v.getFromVersion())));
        v.setSize(CommonSchema.sizeSchema.validate(ctx.push(v.getSize())));
        //v.setFromSequenceNo(sequenceNoSchema.validate(ctx.push(v.getFromSequenceNo())));
        return v.build();
    }
}
