package io.mubel.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class EnumSchema<T extends Enum> extends FieldSchema<T> {

    protected EnumSchema(final String field, final List<Validator<T>> validators, Function<T, T> valueModifier) {
        super(field, validators, valueModifier);
    }

    public static Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder<T extends Enum> {

        private final String field;
        private T defaultValue;
        private boolean required;

        private Builder(final String field) {
            this.field = field;
        }

        public Builder<T> defaultValue(T v) {
            defaultValue = v;
            return this;
        }

        public EnumSchema<T> required() {
            required = true;
            return build();
        }

        public EnumSchema<T> build() {
            Function<T, T> valueModifier = null;
            if (defaultValue != null) {
                valueModifier = (input) -> input == null ? defaultValue : input;
            }
            var validators = new ArrayList<Validator<T>>(1);
            if (required) {
                validators.add(Validators::required);
            }
            return new EnumSchema<>(field, validators, valueModifier);
        }
    }
}
