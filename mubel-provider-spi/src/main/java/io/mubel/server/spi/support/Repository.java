package io.mubel.server.spi.support;

import java.util.List;
import java.util.Optional;

public interface Repository<T> {

    List<T> getAll();

    T put(T value);

    T get(String key);

    Optional<T> find(String key);

    void remove(String key);

    boolean exists(String key);

}
