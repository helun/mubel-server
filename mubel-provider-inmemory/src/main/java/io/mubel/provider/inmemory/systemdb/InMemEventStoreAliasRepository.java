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

import io.mubel.server.spi.systemdb.EventStoreAliasRepository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemEventStoreAliasRepository implements EventStoreAliasRepository {

    private final Map<String, String> aliasToEsid = new ConcurrentHashMap<>();
    private final Map<String, String> esidToAlias = new ConcurrentHashMap<>();

    @Override
    public String getEventStoreId(String esidOrAlias) {
        var esid = aliasToEsid.get(esidOrAlias);
        return esid != null ? esid : esidOrAlias;
    }

    @Override
    public void setAlias(String eventStoreId, String alias) {
        removeAlias(alias);
        aliasToEsid.put(alias, eventStoreId);
        esidToAlias.put(eventStoreId, alias);
    }

    @Override
    public void removeAlias(String alias) {
        var esid = aliasToEsid.remove(alias);
        if (esid != null) {
            esidToAlias.remove(esid);
        }
    }

    @Override
    public String getAlias(String esid) {
        var alias = esidToAlias.get(esid);
        return alias != null ? alias : esid;
    }
}
