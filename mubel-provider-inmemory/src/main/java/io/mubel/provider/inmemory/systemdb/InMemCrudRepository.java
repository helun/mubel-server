/*
 * mubel-provider-inmemory - Multi Backend Event Log
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
