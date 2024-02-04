package io.mubel.provider.inmemory.systemdb;

import io.mubel.provider.test.systemdb.EventStoreDetailsRepositoryTestBase;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;

class InMemEventStoreDetailsRepositoryTest extends EventStoreDetailsRepositoryTestBase {

    static EventStoreDetailsRepository repository = new InMemEventStoreDetailsRepository();

    @Override
    protected EventStoreDetailsRepository repository() {
        return repository;
    }
}