package io.mubel.provider.inmemory.systemdb;

import io.mubel.provider.test.systemdb.JobStatusRepositoryTestBase;
import io.mubel.server.spi.systemdb.JobStatusRepository;

class InMemJobStatusRepositoryTest extends JobStatusRepositoryTestBase {

    static JobStatusRepository repository = new InMemJobStatusRepository();

    @Override
    protected JobStatusRepository repository() {
        return repository;
    }
}