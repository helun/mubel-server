package io.mubel.server.api.grpc.schema;

import com.google.protobuf.Descriptors;
import io.mubel.api.grpc.DataFormat;
import io.mubel.api.grpc.ProvisionEventStoreRequest;
import io.mubel.schema.ObjectSchema;
import io.mubel.schema.Schema;
import io.mubel.schema.StringSchema;
import io.mubel.schema.ValidationContext;

public class ProvisionRequestSchema extends ObjectSchema<ProvisionEventStoreRequest> {

    private final static ProtobufMessageSchema<ProvisionEventStoreRequest> protobufSchema = ProtobufMessageSchema.builder(ProvisionEventStoreRequest.class)
            .requiredField(getFieldDescriptor("esid"))
            .requiredField(getFieldDescriptor("dataFormat"))
            .requiredField(getFieldDescriptor("storageBackendName"))
            .build();

    private static Descriptors.FieldDescriptor getFieldDescriptor(String fieldName) {
        return ProvisionEventStoreRequest.getDescriptor().findFieldByName(fieldName);
    }

    private final static StringSchema esidSchema = CommonSchema.esidSchema.required();
    private final static Schema<DataFormat> dataFormat = Schema.enumSchema("dataFormat")
            .defaultValue(DataFormat.JSON)
            .required();
    private final static StringSchema storageBackendName = CommonSchema.safeString.required();

    public static ProvisionRequestSchema get() {
        return new ProvisionRequestSchema();
    }

    @Override
    protected ProvisionEventStoreRequest doValidate(final ValidationContext<ProvisionEventStoreRequest> ctx) {
        var value = ProvisionEventStoreRequest.newBuilder(protobufSchema.validate(ctx));
        value.setEsid(esidSchema.validate(ctx.push(value.getEsid())));
        value.setDataFormat(dataFormat.validate(ctx.push(value.getDataFormat())));
        value.setStorageBackendName(storageBackendName.validate(ctx.push(value.getStorageBackendName())));
        return value.build();
    }
}
