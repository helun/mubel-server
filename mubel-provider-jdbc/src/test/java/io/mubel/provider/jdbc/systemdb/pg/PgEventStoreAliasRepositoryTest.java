package io.mubel.provider.jdbc.systemdb.pg;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.systemdb.JdbcEventStoreAliasRepository;
import io.mubel.provider.jdbc.systemdb.SystemDbMigrator;
import io.mubel.provider.test.systemdb.EventStoreAliasRepositoryTestBase;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import org.jdbi.v3.core.Jdbi;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

@Testcontainers
public class PgEventStoreAliasRepositoryTest extends EventStoreAliasRepositoryTestBase {

    @Container
    static PostgreSQLContainer<?> container = Containers.postgreSQLContainer();

    static JdbcEventStoreAliasRepository repository;
    static Jdbi jdbi;

    @BeforeAll
    static void setup() throws Exception {
        var dataSource = Containers.dataSource(container);
        jdbi = Jdbi.create(dataSource);
        CacheManager cacheManager = getCacheManager();
        SystemDbMigrator.migrator(container.getJdbcUrl(), container.getUsername(), container.getPassword())
                .migrate();
        repository = new JdbcEventStoreAliasRepository(jdbi, cacheManager);
        repository.afterPropertiesSet();
    }

    @NotNull
    private static SimpleCacheManager getCacheManager() {
        var cm = new SimpleCacheManager();
        var caches = List.of(
                new ConcurrentMapCache(JdbcEventStoreAliasRepository.ALIAS_TO_ESID_CACHE_NAME),
                new ConcurrentMapCache(JdbcEventStoreAliasRepository.ESID_TO_ALIAS_CACHE_NAME)
        );
        cm.setCaches(caches);
        cm.initializeCaches();
        return cm;
    }

    @AfterEach
    void tearDown() {
        jdbi.useHandle(handle -> handle.execute("DELETE FROM event_store_alias"));
    }

    @Override
    protected EventStoreAliasRepository repository() {
        return repository;
    }
}
