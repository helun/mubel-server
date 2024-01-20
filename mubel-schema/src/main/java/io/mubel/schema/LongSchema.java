package io.mubel.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class LongSchema extends FieldSchema<Long> {

    private static final Long INT_DEFAULT_VALUE = 0L;

    private LongSchema(
            final String field,
            final List<Validator<Long>> validators,
            final Function<Long, Long> valueModifier
    ) {
        super(field, validators, valueModifier);
    }

    public static Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder {
        private final String field;
        private Long defaultValue;
        private Long minInclusive;
        private Long maxExclusive;

        private Builder(final String field) {
            this.field = field;
        }

        public Builder defaultValue(long v) {
            defaultValue = v;
            return this;
        }

        public Builder minInclusive(long v) {
            minInclusive = v;
            return this;
        }

        public Builder maxExclusive(long v) {
            maxExclusive = v;
            return this;
        }

        public Builder positive() {
            minInclusive = 0L;
            return this;
        }

        public LongSchema build() {
            Function<Long, Long> valueModifier = null;
            if (defaultValue != null) {
                valueModifier = (input) -> input.equals(INT_DEFAULT_VALUE) ? defaultValue : input;
            }
            var validators = new ArrayList<Validator<Long>>(2);
            if (minInclusive != null) {
                validators.add(ctx -> Validators.minInclusive(ctx, minInclusive));
            }
            if (maxExclusive != null) {
                validators.add(ctx -> Validators.maxExclusive(ctx, maxExclusive));
            }
            return new LongSchema(field, validators, valueModifier);
        }
    }
}
