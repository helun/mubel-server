package io.mubel.server.mubelserver.api.grpc;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.*;
import io.mubel.server.mubelserver.eventstore.EventStoreManager;
import io.mubel.server.mubelserver.eventstore.ProvisionService;
import io.mubel.server.mubelserver.jobs.BackgroundJobService;
import io.mubel.server.mubelserver.support.IdGenerator;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import org.springframework.stereotype.Service;

import java.util.function.BiFunction;

@Service
public class EventStoreApiService {

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
        eventStoreManager.resolveEventStore(request.getEsid())
                .append(request);
        responseObserver.onNext(APPEND_ACK);
        responseObserver.onCompleted();
    }

    public void provision(ProvisionEventStoreRequest request, StreamObserver<JobStatus> responseObserver) {
        var command = new ProvisionCommand(
                idGenerator.generate(),
                request.getEsid(),
                request.getDataFormat(),
                request.getStorageBackendName()
        );
        provisionService.provision(command);
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
        var command = new DropEventStoreCommand(
                idGenerator.generate(),
                request.getEsid()
        );
        provisionService.drop(command);
        jobService.awaitJobStatus(command.jobId())
                .handle(replyWithJobStatus(responseObserver));
    }

    public void get(GetEventsRequest request, StreamObserver<GetEventsResponse> responseObserver) {
        final var result = eventStoreManager.resolveEventStore(request.getEsid())
                .get(request);
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

        responseObserver.onNext(ServiceInfoResponse.newBuilder()
                .addAllEventStore(details)
                .addAllStorageBackend(backends)
                .build());
        responseObserver.onCompleted();
    }
}
