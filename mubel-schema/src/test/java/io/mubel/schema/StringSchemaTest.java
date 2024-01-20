package io.mubel.schema;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.mubel.schema.Constrains.PROPERTY_PATH_PTRN;
import static org.assertj.core.api.Assertions.assertThat;

class StringSchemaTest {

    @Test
    void pattern() {
        var schema = Schema.string("test")
                .pattern("alpha-numeric", "^[a-z0-9_-]+$")
                .build();
        final String safeString = "abc_123";
        var ctx = ValidationContext.create(safeString);
        var parsed = schema.validate(ctx);
        assertThat(parsed).isEqualTo(safeString);
        assertThat(ctx.hasErrors()).isFalse();

        var unsafeString = "${ some shit~}";
        var parsed2 = schema.validate(ctx.push(unsafeString));
        assertThat(parsed2).isEqualTo(unsafeString);
        assertThat(ctx.errors()).contains(new ValidationError(
                "test does not conform to alpha-numeric pattern: ^[a-z0-9_-]+$"));
    }

    @Test
    void uuid() {
        var schema = Schema.string("test")
                .uuid()
                .build();
        final String uuidString = UUID.randomUUID().toString();
        var ctx = ValidationContext.create(uuidString);
        var parsed = schema.validate(ctx);
        assertThat(parsed).isEqualTo(uuidString);
        assertThat(ctx.hasErrors()).isFalse();

        final var notUuid = "12345-4321";
        var parsed2 = schema.validate(ctx.push(notUuid));
        assertThat(parsed2).isEqualTo(notUuid);
        assertThat(ctx.errors())
                .allMatch(err -> err.message().startsWith("test does not conform to uuid pattern:"));
    }

    @Test
    void propertyPath() {
        var schema = Schema.string("test")
                .pattern("property", PROPERTY_PATH_PTRN)
                .build();

        final String valid1 = "Test.Event1234";
        var ctx = ValidationContext.create(valid1);
        var parsed = schema.validate(ctx);
        assertThat(parsed).isEqualTo(valid1);
        ctx.printErrors();
        assertThat(ctx.hasErrors()).isFalse();

        final var invalid = "invliad.{sdd}";
        var parsed2 = schema.validate(ctx.push(invalid));
        assertThat(parsed2).isEqualTo(invalid);
        assertThat(ctx.errors())
                .allMatch(err -> err.message().startsWith("test does not conform to property pattern:"));
    }

}