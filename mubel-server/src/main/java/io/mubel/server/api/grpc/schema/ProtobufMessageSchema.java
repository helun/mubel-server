package io.mubel.server.api.grpc.schema;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import io.mubel.schema.Schema;
import io.mubel.schema.ValidationContext;
import io.mubel.schema.ValidationError;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ProtobufMessageSchema<T extends GeneratedMessageV3> extends Schema<T> {

    public static <T extends GeneratedMessageV3> Builder<T> builder(Class<T> klass) {
        return new Builder<>(klass);
    }

    private final List<Descriptors.FieldDescriptor> requiredFields;

    private ProtobufMessageSchema(Builder<T> builder) {
        this.requiredFields = List.copyOf(builder.requiredFields);
    }

    @Override
    protected T doValidate(ValidationContext<T> ctx) {
        var value = ctx.currentValue();
        for (var field : requiredFields) {
            ctx.pushPath(field.getName());
            if (!value.hasField(field)) {
                ctx.addError(Optional.of(new ValidationError(ctx.currentPath() + " is required")));
            }
            ctx.pop();
        }
        return value;
    }

    public static class Builder<T extends GeneratedMessageV3> {
        private final Class<T> klass;
        private final List<Descriptors.FieldDescriptor> requiredFields = new ArrayList<>();


        private Builder(final Class<T> klass) {
            this.klass = klass;
        }

        public Builder<T> requiredField(Descriptors.FieldDescriptor field) {
            requiredFields.add(requireNonNull(field));
            return this;
        }

        public ProtobufMessageSchema<T> build() {
            return new ProtobufMessageSchema<>(this);
        }

    }

}
