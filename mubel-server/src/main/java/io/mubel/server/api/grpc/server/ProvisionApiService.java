package io.mubel.server.api.grpc.server;

import io.mubel.api.grpc.v1.server.DropEventStoreRequest;
import io.mubel.api.grpc.v1.server.JobStatus;
import io.mubel.api.grpc.v1.server.ProvisionEventStoreRequest;
import io.mubel.server.api.grpc.Validator;
import io.mubel.server.eventstore.ProvisionService;
import io.mubel.server.jobs.BackgroundJobService;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.support.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class ProvisionApiService {

    private static final Logger LOG = LoggerFactory.getLogger(ProvisionApiService.class);

    private final ProvisionService provisionService;
    private final BackgroundJobService jobService;
    private final IdGenerator idGenerator;

    public ProvisionApiService(ProvisionService provisionService, BackgroundJobService jobService, IdGenerator idGenerator) {
        this.provisionService = provisionService;
        this.jobService = jobService;
        this.idGenerator = idGenerator;
    }

    public CompletableFuture<JobStatus> provision(ProvisionEventStoreRequest request) {
        var validated = Validator.validate(request);
        var command = new ProvisionCommand(
                idGenerator.generateStringId(),
                validated.getEsid(),
                validated.getDataFormat(),
                validated.getStorageBackendName()
        );
        provisionService.provision(command)
                .handleAsync((ignored, err) -> {
                    if (err != null) {
                        LOG.error("provision failed: {}", err.getMessage(), err);
                    } else {
                        LOG.info("provision succeeded: {}", command);
                    }
                    return null;
                });
        return jobService.awaitJobStatus(command.jobId());
    }

    public CompletableFuture<JobStatus> drop(DropEventStoreRequest request) {
        var validated = Validator.validate(request);
        var command = new DropEventStoreCommand(
                idGenerator.generateStringId(),
                validated.getEsid()
        );
        provisionService.drop(command);
        return jobService.awaitJobStatus(command.jobId());
    }
}
