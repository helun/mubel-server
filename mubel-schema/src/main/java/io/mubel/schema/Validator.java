package io.mubel.schema;

import java.util.Optional;
import java.util.function.Function;

public interface Validator<T> extends Function<ValidationContext<T>, Optional<ValidationError>> {

}
