package io.mubel.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ValidationContext<T> {

    private final int MAX_ERRORS = 100;

    private PropertyPath path = new PropertyPath();
    private Object currentValue;
    private List<ValidationError> errors;

    public static <T> ValidationContext<T> create(T valueToValidate) {
        return new ValidationContext(valueToValidate);
    }

    private ValidationContext(T valueToValidate) {
        this.currentValue = valueToValidate;
    }

    public List<ValidationError> errors() {
        return errors != null ? errors : List.of();
    }

    public boolean hasErrors() {
        return errors != null && !errors.isEmpty();
    }

    public <N> ValidationContext<N> push(N valueToValidate) {
        currentValue = valueToValidate;
        return (ValidationContext<N>) this;
    }

    public void pushPath(String field) {
        path.push(field);
    }

    public void pushPath(int arrayIndex) {
        path.push(arrayIndex);
    }

    public void pop() {
        path.pop();
        currentValue = null;
    }

    public T currentValue() {
        return (T) currentValue;
    }

    public String currentPath() {
        return path.toString();
    }

    public void addError(final Optional<ValidationError> error) {
        if (error.isPresent()) {
            errors = Objects.requireNonNullElseGet(errors, ArrayList::new);
            if (errors.size() < MAX_ERRORS) {
                errors.add(error.get());
            }
        }
    }

    public boolean maxErrorsReached() {
        return errors != null && errors.size() >= MAX_ERRORS;
    }

    public void setCurrentValue(final T newValue) {
        this.currentValue = newValue;
    }

    public void printErrors() {
        errors().forEach(System.err::println);
    }
}
