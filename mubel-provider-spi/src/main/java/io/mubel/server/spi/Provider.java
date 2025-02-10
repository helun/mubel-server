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
package io.mubel.server.spi;

import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.StorageBackendProperties;

import java.util.Set;

public interface Provider {

    String name();

    Set<StorageBackendProperties> storageBackends();

    /**
     * Provision an event store.
     * The provider is responsible for creating the event store.
     *
     * @param command
     */
    void provision(ProvisionCommand command);

    void drop(DropEventStoreCommand command);

    StorageBackendProperties getStorageBackend(String storageBackendName);

    EventStoreContext openEventStore(String esid);

    void closeEventStore(String esid);
}
