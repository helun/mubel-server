package io.mubel.schema;

import java.util.List;

public record ValidationResult<T>(T value, List<ValidationError> errors) {

    public static <T> ValidationResult<T> success(T subject) {
        return new ValidationResult<>(subject, null);
    }

    public static <T> ValidationResult<T> failure(T subject, List<ValidationError> errors) {
        return new ValidationResult<>(subject, errors);
    }

    public List<ValidationError> errors() {
        if (errors != null) {
            return errors;
        }
        return List.of();
    }

    public boolean hasErrors() {
        return errors != null && !errors.isEmpty();
    }

}
