package io.mubel.provider.inmemory.systemdb;

import io.mubel.api.grpc.v1.server.JobStatus;
import io.mubel.server.spi.systemdb.JobStatusRepository;

public class InMemJobStatusRepository extends InMemCrudRepository<JobStatus> implements JobStatusRepository {

    @Override
    public JobStatus put(JobStatus value) {
        return data.compute(value.getJobId(), (k, existing) -> {
            if (existing != null) {
                return value.toBuilder()
                        .setCreatedAt(existing.getCreatedAt())
                        .build();
            }
            return value;
        });
    }

}
