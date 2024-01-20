package io.mubel.schema;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

public class Validators {

    public static Validator<String> patternValidator(final String patternName, final String pattern) {
        final var name = Objects.requireNonNull(patternName);
        final var patternPredicate = Pattern
                .compile(Objects.requireNonNull(pattern))
                .asMatchPredicate();

        return ctx -> {
            var value = ctx.currentValue();
            if (Constrains.isNotBlank(value) && !patternPredicate.test(value)) {
                var error = new ValidationError("%s does not conform to %s pattern: %s".formatted(
                        ctx.currentPath(),
                        name,
                        pattern
                ));
                return Optional.of(error);
            }
            return Optional.empty();
        };
    }

    public static Optional<ValidationError> required(ValidationContext<?> ctx) {
        var value = ctx.currentValue();
        if (!Constrains.isNonNull(value) || (value instanceof String s && !Constrains.isNotBlank(s))) {
            return Optional.of(new ValidationError(ctx.currentPath() + " is required"));
        }
        return Optional.empty();
    }

    public static Optional<ValidationError> minInclusive(ValidationContext<Integer> ctx, int minInclusive) {
        int value = ctx.currentValue();
        if (value < minInclusive) {
            return Optional.of(new ValidationError("%s should be >= than %s but was %d".formatted(
                    ctx.currentPath(),
                    minInclusive,
                    value
            )));
        }
        return Optional.empty();
    }

    public static Optional<ValidationError> minInclusive(ValidationContext<Long> ctx, long minInclusive) {
        long value = ctx.currentValue();
        if (value < minInclusive) {
            return Optional.of(new ValidationError("%s should be >= than %s but was %d".formatted(
                    ctx.currentPath(),
                    minInclusive,
                    value
            )));
        }
        return Optional.empty();
    }

    public static Optional<ValidationError> maxExclusive(
            final ValidationContext<Integer> ctx,
            final Integer maxExclusive
    ) {
        int value = ctx.currentValue();
        if (value >= maxExclusive) {
            return Optional.of(new ValidationError("%s should be < %d but was %d".formatted(
                    ctx.currentPath(),
                    maxExclusive,
                    value
            )));
        }
        return Optional.empty();
    }

    public static Optional<ValidationError> maxExclusive(
            final ValidationContext<Long> ctx,
            final Long maxExclusive
    ) {
        long value = ctx.currentValue();
        if (value >= maxExclusive) {
            return Optional.of(new ValidationError("%s should be < %d but was %d".formatted(
                    ctx.currentPath(),
                    maxExclusive,
                    value
            )));
        }
        return Optional.empty();
    }
}
