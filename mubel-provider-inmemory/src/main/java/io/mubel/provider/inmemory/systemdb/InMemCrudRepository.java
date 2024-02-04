package io.mubel.provider.inmemory.systemdb;

import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.support.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public abstract class InMemCrudRepository<T> implements Repository<T> {
    protected final Map<String, T> data = new ConcurrentHashMap<>();

    @Override
    public T get(String key) {
        var value = data.get(key);
        if (value == null) {
            throw new ResourceNotFoundException("Value not found: " + key);
        }
        return value;
    }

    @Override
    public boolean exists(String key) {
        return data.containsKey(key);
    }

    @Override
    public Optional<T> find(String key) {
        return Optional.ofNullable(data.get(key));
    }

    @Override
    public List<T> getAll() {
        return new ArrayList<>(data.values());
    }

    @Override
    public void remove(String key) {
        data.remove(key);
    }
}
