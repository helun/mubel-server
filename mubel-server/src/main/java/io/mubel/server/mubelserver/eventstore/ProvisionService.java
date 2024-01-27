package io.mubel.server.mubelserver.eventstore;

import io.mubel.api.grpc.JobState;
import io.mubel.api.grpc.JobStatus;
import io.mubel.api.grpc.ProblemDetail;
import io.mubel.server.mubelserver.Providers;
import io.mubel.server.spi.eventstore.EventStoreState;
import io.mubel.server.spi.exceptions.ResourceConflictException;
import io.mubel.server.spi.exceptions.ResourceNotFoundException;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import io.mubel.server.spi.model.StorageBackendProperties;
import io.mubel.server.spi.systemdb.EventStoreAliasRepository;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Service
public class ProvisionService {
    private static final Logger LOG = LoggerFactory.getLogger(ProvisionService.class);
    private final Providers providers;
    private final EventStoreDetailsRepository detailsRepository;
    private final EventStoreAliasRepository aliases;
    private final ApplicationEventPublisher publisher;

    public ProvisionService(
            Providers providers,
            EventStoreDetailsRepository detailsRepository,
            EventStoreAliasRepository aliases,
            ApplicationEventPublisher publisher
    ) {
        this.providers = providers;
        this.detailsRepository = detailsRepository;
        this.aliases = aliases;
        this.publisher = publisher;
    }

    @Async
    public Future<Void> provision(ProvisionCommand command) {
        final var job = JobStatus.newBuilder()
                .setJobId(command.jobId())
                .setDescription("Provision event store %s".formatted(command.esid()))
                .setState(JobState.RUNNING)
                .setCreatedAt(System.currentTimeMillis())
                .build();
        publisher.publishEvent(job);
        try {
            return doProvision(command, job);
        } catch (RuntimeException e) {
            publishFailed(e, job);
            throw e;
        }
    }

    private void publishFailed(RuntimeException e, JobStatus job) {
        var problem = ProblemDetail.newBuilder()
                .setType("https://mubel.io/problems/provision-failed")
                .setStatus(500)
                .setTitle("Provision failed")
                .setDetail(e.getMessage())
                .build();

        publisher.publishEvent(
                job.toBuilder()
                        .setProblem(problem)
                        .setUpdatedAt(System.currentTimeMillis())
                        .setState(JobState.FAILED)
                        .build()
        );
    }

    private CompletableFuture<Void> doProvision(ProvisionCommand command, JobStatus job) {
        checkProvisionPrerequisites(command);
        final var provider = providers.findBackend(command.storageBackendName())
                .orElseThrow(() -> new ResourceNotFoundException("No provider for backend name: " + command.storageBackendName()));
        final var backend = provider.getStorageBackend(command.storageBackendName());
        final var details = saveInitialProvisionState(command, backend);
        provider.provision(command);
        saveFinalProvisionState(details, job);
        return CompletableFuture.completedFuture(null);
    }

    private void checkProvisionPrerequisites(ProvisionCommand request) {
        if (detailsRepository.exists(request.esid())) {
            throw new ResourceConflictException("event store with id %s already exists".formatted(request.esid()));
        }
        final var backend = request.storageBackendName();
        if (!providers.backendExists(backend)) {
            throw new ResourceNotFoundException("storage backend %s is not configured".formatted(backend));
        }
    }

    private SpiEventStoreDetails saveInitialProvisionState(ProvisionCommand request, StorageBackendProperties backend) {
        final var details = new SpiEventStoreDetails(
                request.esid(),
                backend.provider(),
                backend.type(),
                request.dataFormat(),
                EventStoreState.PRE_PROVISION
        );
        return detailsRepository.put(details);
    }

    private void saveFinalProvisionState(SpiEventStoreDetails details, JobStatus job) {
        final var d = details.withState(EventStoreState.PROVISIONED);
        LOG.debug("saving final provision state for {}", d);
        detailsRepository.put(d);
        publishCompleted(job);
    }

    @Async
    public Future<Void> drop(DropEventStoreCommand command) {
        final var job = JobStatus.newBuilder()
                .setCreatedAt(System.currentTimeMillis())
                .setJobId(command.jobId())
                .setDescription("Drop event store %s".formatted(command.esid()))
                .setState(JobState.RUNNING)
                .build();
        publisher.publishEvent(job);
        try {
            final var esid = aliases.getEventStoreId(command.esid());
            final var details = detailsRepository.get(esid);
            final var provider = providers.get(details.provider());
            provider.drop(command);
            publishCompleted(job);
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException e) {
            publishFailed(e, job);
            throw e;
        }
    }

    private void publishCompleted(JobStatus job) {
        publisher.publishEvent(
                job.toBuilder()
                        .setUpdatedAt(System.currentTimeMillis())
                        .setState(JobState.COMPLETED)
                        .build());
    }
}
