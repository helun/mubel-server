/*
 * mubel-provider-spi - Multi Backend Event Log
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
package io.mubel.server.spi.queue;

import io.mubel.server.spi.exceptions.ResourceNotFoundException;

import java.util.List;
import java.util.Optional;

public record QueueConfigurations(
        List<QueueConfiguration> queues
) {

    public QueueConfiguration getQueue(String name, String defaults) {
        return findConfig(name)
                .or(() -> findConfig(defaults))
                .map(config -> {
                    if (name.equals(config.name())) {
                        return config;
                    }
                    return new QueueConfiguration(name,
                            config.visibilityTimeout(),
                            config.polIInterval()
                    );
                })
                .orElseThrow(() -> new ResourceNotFoundException("Queue not found: " + name));
    }

    private Optional<QueueConfiguration> findConfig(String name) {
        return queues.stream()
                .filter(queue -> queue.name().equals(name))
                .findFirst();
    }

}
