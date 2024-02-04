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
