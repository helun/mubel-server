package io.mubel.server.spi.systemdb;

public interface EventStoreAliasRepository {

    String getEventStoreId(String alias);

    void setAlias(String eventStoreId, String alias);

    void removeAlias(String alias);

    String getAlias(String esid);
}
