package io.mubel.server.api.grpc;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.*;
import io.mubel.server.eventstore.EventStoreManager;
import io.mubel.server.eventstore.ProvisionService;
import io.mubel.server.jobs.BackgroundJobService;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.support.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.function.BiFunction;

@Service
public class EventStoreApiService {

    private static final Logger LOG = LoggerFactory.getLogger(EventStoreApiService.class);
    private static final AppendAck APPEND_ACK = AppendAck.newBuilder().build();
    private final EventStoreManager eventStoreManager;
    private final ProvisionService provisionService;
    private final BackgroundJobService jobService;
    private final IdGenerator idGenerator;

    public EventStoreApiService(
            EventStoreManager eventStoreManager,
            ProvisionService provisionService,
            BackgroundJobService jobService,
            IdGenerator idGenerator
    ) {
        this.eventStoreManager = eventStoreManager;
        this.provisionService = provisionService;
        this.jobService = jobService;
        this.idGenerator = idGenerator;
    }

    public void append(AppendRequest request, StreamObserver<AppendAck> responseObserver) {
        Validator.validate(request);
        LOG.debug("append: {} events", request.getEventCount());
        eventStoreManager.resolveEventStore(request.getEsid())
                .append(request);
        responseObserver.onNext(APPEND_ACK);
        responseObserver.onCompleted();
    }

    public void provision(ProvisionEventStoreRequest request, StreamObserver<JobStatus> responseObserver) {
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
        jobService.awaitJobStatus(command.jobId())
                .handle(replyWithJobStatus(responseObserver));
    }

    private static BiFunction<JobStatus, Throwable, Object> replyWithJobStatus(StreamObserver<JobStatus> responseObserver) {
        return (status, throwable) -> {
            if (throwable != null) {
                responseObserver.onError(throwable);
            } else {
                responseObserver.onNext(status);
                responseObserver.onCompleted();
            }
            return null;
        };
    }

    public void drop(DropEventStoreRequest request, StreamObserver<JobStatus> responseObserver) {
        var validated = Validator.validate(request);
        var command = new DropEventStoreCommand(
                idGenerator.generateStringId(),
                validated.getEsid()
        );
        provisionService.drop(command);
        jobService.awaitJobStatus(command.jobId())
                .handle(replyWithJobStatus(responseObserver));
    }

    public void get(GetEventsRequest request, StreamObserver<GetEventsResponse> responseObserver) {
        final var validated = Validator.validate(request);
        final var result = eventStoreManager.resolveEventStore(validated.getEsid())
                .get(validated);
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    public void backendInfo(GetServiceInfoRequest request, StreamObserver<ServiceInfoResponse> responseObserver) {
        final var backends = eventStoreManager.providers()
                .stream()
                .flatMap(provider -> provider.storageBackends().stream())
                .sorted((o1, o2) -> o1.name().compareToIgnoreCase(o2.name()))
                .map(backend -> StorageBackendInfo.newBuilder()
                        .setName(backend.name())
                        .setType(backend.type().name())
                        .build())
                .toList();

        final var details = eventStoreManager.getAllEventStoreDetails();

        ServiceInfoResponse response = ServiceInfoResponse.newBuilder()
                .addAllEventStore(details)
                .addAllStorageBackend(backends)
                .build();
        LOG.debug("server info: {}", response);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void eventStoreSummary(GetEventStoreSummaryRequest request, StreamObserver<EventStoreSummary> responseObserver) {
        final var summary = eventStoreManager.getSummary(request);
        responseObserver.onNext(summary);
        responseObserver.onCompleted();
    }


}
