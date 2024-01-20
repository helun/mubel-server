package io.mubel.schema;

import java.util.ArrayList;
import java.util.List;

public class StringSchema extends FieldSchema<String> {

    private final static String UUID_PRTN = "^[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b"
                                                + "-[0-9a-fA-F]{12}$";

    private StringSchema(
        final String field,
        final List<Validator<String>> validators
    ) {
        super(field, validators);
    }

    public static Builder builder(String field) {
        return new Builder(field);
    }

    public static class Builder {
        private String field;
        private boolean required;
        private String pattern;
        private String patternName;

        private Builder(final String field) {
            this.field = field;
        }

        public Builder field(String s) {
            this.field = s;
            return this;
        }

        public Builder pattern(String patternName, String pattern) {
            this.patternName = patternName;
            this.pattern = pattern;
            return this;
        }

        public Builder uuid() {
            return pattern("uuid", UUID_PRTN);
        }

        public StringSchema required() {
            required = true;
            return build();
        }

        public StringSchema build() {
            var validators = new ArrayList<Validator<String>>(1);
            if (required) {
                validators.add(Validators::required);
            }
            if (pattern != null) {
                validators.add(Validators.patternValidator(patternName, pattern));
            }
            return new StringSchema(field, validators);
        }

    }
}
