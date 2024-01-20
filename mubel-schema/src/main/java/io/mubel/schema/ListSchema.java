package io.mubel.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class ListSchema<T> extends Schema<List<T>> {

    private final String field;
    private final List<CollectionValidator> listValidators;
    private final Schema<T> elementSchema;

    private ListSchema(
            final String field,
            final List<CollectionValidator> validators,
            final Schema<T> elementSchema
    ) {
        this.field = field;
        this.listValidators = validators;
        this.elementSchema = elementSchema;
    }

    public static <T> Builder<T> builder(String field) {
        return new Builder<>(field);
    }

    @Override
    protected void preValidate(final ValidationContext<List<T>> ctx) {
        ctx.pushPath(field);
    }

    @Override
    public List<T> doValidate(final ValidationContext<List<T>> ctx) {
        var list = ctx.currentValue();
        for (var v : listValidators) {
            v.apply(ctx);
        }
        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                var element = list.get(i);
                ctx.pushPath(i);
                elementSchema.validate(ctx.push(element));
                ctx.pop();
            }
        }
        return list;
    }

    @Override
    protected void postValidate(final ValidationContext<List<T>> ctx) {
        ctx.pop();
    }

    private interface CollectionValidator
            extends Function<ValidationContext<? extends Collection<?>>, Optional<ValidationError>> {
    }

    public static class Builder<T> {
        private final String field;
        private Schema<T> elementSchema;

        public Builder(final String field) {
            this.field = field;
        }

        public Builder<T> elements(Schema<T> elementSchema) {
            this.elementSchema = elementSchema;
            return this;
        }

        public ListSchema<T> required() {
            var validators = new ArrayList<CollectionValidator>(1);
            return new ListSchema<>(field, validators, elementSchema);
        }

    }
}
