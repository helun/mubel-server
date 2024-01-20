package io.mubel.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class IntSchema extends FieldSchema<Integer> {

    private static final Integer INT_DEFAULT_VALUE = 0;

    private IntSchema(
        final String field,
        final List<Validator<Integer>> validators,
        final Function<Integer, Integer> valueModifier
    ) {
        super(field, validators, valueModifier);
    }

    public static Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder {
        private final String field;
        private Integer defaultValue;
        private Integer minInclusive;
        private Integer maxExclusive;

        private Builder(final String field) {
            this.field = field;
        }

        public Builder defaultValue(int v) {
            defaultValue = v;
            return this;
        }

        public Builder minInclusive(int v) {
            minInclusive = v;
            return this;
        }

        public Builder maxExclusive(int v) {
            maxExclusive = v;
            return this;
        }

        public Builder positive() {
            minInclusive = 0;
            return this;
        }

        public IntSchema build() {
            Function<Integer, Integer> valueModifier = null;
            if (defaultValue != null) {
                valueModifier = (input) -> input.equals(INT_DEFAULT_VALUE) ? defaultValue : input;
            }
            var validators = new ArrayList<Validator<Integer>>(2);
            if (minInclusive != null) {
                validators.add(ctx -> Validators.minInclusive(ctx, minInclusive));
            }
            if (maxExclusive != null) {
                validators.add(ctx -> Validators.maxExclusive(ctx, maxExclusive));
            }
            return new IntSchema(field, validators, valueModifier);
        }
    }
}
