package io.mubel.schema;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class ValidationContextTest {

    @Test
    void maxErrorsShouldNotTrigger() {
        var s = "a value";
        var ctx = ValidationContext.create(s);
        ctx.addError(Optional.of(new ValidationError("an error")));
        assertThat(ctx.maxErrorsReached()).isFalse();
    }

    @Test
    void maxErrorsExceeded() {
        var s = "a value";
        var ctx = ValidationContext.create(s);
        IntStream.range(0, 101).forEach(i -> ctx.addError(Optional.of(new ValidationError("error #%d".formatted(i)))));
        assertThat(ctx.maxErrorsReached()).isTrue();
        assertThat(ctx.errors()).hasSize(100);
    }

}