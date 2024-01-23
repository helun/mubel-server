package io.mubel.server.spi.systemdb;

import java.util.Map;

public interface EventStoreAliasRepository {

    String getEventStoreId(String alias);

    void setAlias(String eventStoreId, String alias);

    void removeAlias(String alias);

    Map<String, String> getAliases();

    String getAlias(String esid);
}
