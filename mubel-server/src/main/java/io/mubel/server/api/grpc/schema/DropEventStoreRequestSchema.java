package io.mubel.server.api.grpc.schema;

import com.google.protobuf.Descriptors;
import io.mubel.api.grpc.v1.server.DropEventStoreRequest;
import io.mubel.schema.ObjectSchema;
import io.mubel.schema.StringSchema;
import io.mubel.schema.ValidationContext;


public class DropEventStoreRequestSchema extends ObjectSchema<DropEventStoreRequest> {

    private final static ProtobufMessageSchema<DropEventStoreRequest> protobufSchema = ProtobufMessageSchema.builder(DropEventStoreRequest.class)
            .requiredField(getFieldDescriptor("esid"))
            .build();

    private static Descriptors.FieldDescriptor getFieldDescriptor(String fieldName) {
        return DropEventStoreRequest.getDescriptor().findFieldByName(fieldName);
    }

    private final static StringSchema esidSchema = CommonSchema.esidSchema.required();

    public static DropEventStoreRequestSchema get() {
        return new DropEventStoreRequestSchema();
    }

    @Override
    protected DropEventStoreRequest doValidate(ValidationContext<DropEventStoreRequest> ctx) {
        var value = DropEventStoreRequest.newBuilder(protobufSchema.validate(ctx));
        value.setEsid(esidSchema.validate(ctx.push(value.getEsid())));
        return value.build();
    }
}
