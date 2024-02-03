package io.mubel.provider.jdbc.systemdb;

import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

public class JdbcEventStoreAliasRepository implements EventStoreAliasRepository, InitializingBean {

    public static final String INSERT = """
            INSERT INTO event_store_alias(esid, alias)
            VALUES (?, ?)
            """;

    public static final String EXISTS = """
            SELECT COUNT(*) FROM event_store_alias
            WHERE esid = ?
            """;

    public static final String UPDATE = """
            UPDATE event_store_alias
            SET alias = ?
            WHERE esid = ?
            """;
    public static final String SELECT_ALL = "SELECT * FROM event_store_alias";
    public static final String DELETE = """
                DELETE FROM event_store_alias
                WHERE alias = ?
            """;
    public static final String ALIAS_TO_ESID_CACHE_NAME = "alias_to_esid";
    public static final String ESID_TO_ALIAS_CACHE_NAME = "esit_to_alias";

    private final Jdbi jdbi;
    private final CacheManager cacheManager;

    private Cache aliasToEsidCache;
    private Cache esidToAliasCache;

    public JdbcEventStoreAliasRepository(Jdbi jdbi, CacheManager cacheManager) {
        this.jdbi = jdbi;
        this.cacheManager = cacheManager;
    }

    @Override
    public String getEventStoreId(String esidOrAlias) {
        var esid = aliasToEsidCache.get(esidOrAlias, String.class);
        return esid != null ? esid : esidOrAlias;
    }

    @Override
    public synchronized void setAlias(String eventStoreId, String alias) {
        removeAlias(alias);
        if (esidExists(eventStoreId)) {
            updateAlias(eventStoreId, alias);
        } else {
            insert(eventStoreId, alias);
        }
        refreshCaches();
    }

    private void refreshCaches() {
        aliasToEsidCache.clear();
        esidToAliasCache.clear();
        jdbi.useHandle(handle -> {
            handle.select(SELECT_ALL)
                    .mapToMap()
                    .forEach(row -> {
                        var alias = (String) row.get("alias");
                        var eventStoreId = (String) row.get("esid");

                        aliasToEsidCache.put(alias, eventStoreId);
                        esidToAliasCache.put(eventStoreId, alias);
                    });
        });
    }

    private boolean esidExists(String eventStoreId) {
        Integer count = jdbi.withHandle(h -> h.select(EXISTS)
                .bind(0, eventStoreId)
                .mapTo(Integer.class)
                .one());
        return count != null && count > 0;
    }

    private void insert(String eventStoreId, String alias) {
        jdbi.useHandle(h -> h.createUpdate(INSERT)
                .bind(0, eventStoreId)
                .bind(1, alias)
                .execute());
    }

    public void updateAlias(String eventStoreId, String newAlias) {
        jdbi.useHandle(h -> h.createUpdate(UPDATE)
                .bind(0, newAlias)
                .bind(1, eventStoreId)
                .execute());
    }

    @Override
    public synchronized void removeAlias(String alias) {
        jdbi.useHandle(h -> h.createUpdate(DELETE)
                .bind(0, alias)
                .execute());
        refreshCaches();
    }

    @Override
    public String getAlias(String esid) {
        var alias = esidToAliasCache.get(esid, String.class);
        return alias != null ? alias : esid;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        aliasToEsidCache = cacheManager.getCache(ALIAS_TO_ESID_CACHE_NAME);
        esidToAliasCache = cacheManager.getCache(ESID_TO_ALIAS_CACHE_NAME);
    }
}