package io.mubel.schema;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public abstract class FieldSchema<T> extends Schema<T>{

    protected final String field;
    private final List<Validator<T>> validators;
    private final Function<T, T> valueModifier;

    protected FieldSchema(
        final String field,
        final List<Validator<T>> validators
    ) {
        this(field, validators, null);
    }

    protected FieldSchema(
        final String field,
        final List<Validator<T>> validators,
        final Function<T, T> valueModifier
    ) {
        this.field = Constrains.requireNotBlank(field);
        this.validators = Objects.requireNonNull(validators);
        this.valueModifier = valueModifier;
    }

    @Override
    protected void preValidate(final ValidationContext<T> ctx) {
        ctx.pushPath(field);
        if (valueModifier != null) {
            ctx.setCurrentValue(valueModifier.apply(ctx.currentValue()));
        }
    }

    protected T doValidate(ValidationContext<T> ctx) {
        for (var v : validators) {
            ctx.addError(v.apply(ctx));
        }
        return ctx.currentValue();
    }

    @Override
    protected void postValidate(final ValidationContext<T> ctx) {
        ctx.pop();
    }
}
