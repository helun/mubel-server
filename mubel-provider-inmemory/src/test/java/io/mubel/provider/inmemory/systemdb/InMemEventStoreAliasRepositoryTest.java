package io.mubel.provider.inmemory.systemdb;

import io.mubel.provider.test.systemdb.EventStoreAliasRepositoryTestBase;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;

class InMemEventStoreAliasRepositoryTest extends EventStoreAliasRepositoryTestBase {

    static EventStoreAliasRepository repository = new InMemEventStoreAliasRepository();

    @Override
    protected EventStoreAliasRepository repository() {
        return repository;
    }
}