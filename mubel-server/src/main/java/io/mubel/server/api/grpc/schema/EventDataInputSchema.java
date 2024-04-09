package io.mubel.server.api.grpc.schema;

import io.mubel.api.grpc.v1.events.EventDataInput;
import io.mubel.schema.*;

import static io.mubel.schema.Constrains.EVENT_TYPE_PTRN;
import static io.mubel.server.api.grpc.schema.CommonSchema.*;

public class EventDataInputSchema extends ObjectSchema<EventDataInput> {

    private final static StringSchema idSchema = Schema.string("id").uuid().required();
    private final static StringSchema streamId = streamIdSchema.required();
    private final static IntSchema version = versionSchema;
    private final static StringSchema typeSchema =
            safeString.field("type").pattern("property-path", EVENT_TYPE_PTRN).required();

    public static Schema get() {
        return new EventDataInputSchema();
    }

    public EventDataInput doValidate(final ValidationContext<EventDataInput> ctx) {
        var event = ctx.currentValue();
        idSchema.validate(ctx.push(event.getId()));
        streamId.validate(ctx.push(event.getStreamId()));
        version.validate(ctx.push(event.getRevision()));
        typeSchema.validate(ctx.push(event.getType()));
        return event;
    }
}
