package io.mubel.schema;

public abstract class Schema<T> {

    public static StringSchema.Builder string(String field) {
        return StringSchema.builder(field);
    }

    public static IntSchema.Builder integer(String field) {
        return IntSchema.builder(field);
    }

    public static <T extends Enum> EnumSchema.Builder enumSchema(String field) {
        return EnumSchema.builder(field);
    }

    public static <T extends Schema> ListSchema.Builder<T> list(String field) {
        return ListSchema.builder(field);
    }

    public T validate(ValidationContext<T> ctx) {
        if (ctx.maxErrorsReached()) {
            return ctx.currentValue();
        }
        preValidate(ctx);
        T val = doValidate(ctx);
        postValidate(ctx);
        return val;
    }

    protected void preValidate(ValidationContext<T> ctx) {}

    protected void postValidate(ValidationContext<T> ctx) {}

    protected abstract T doValidate(ValidationContext<T> ctx);

}
