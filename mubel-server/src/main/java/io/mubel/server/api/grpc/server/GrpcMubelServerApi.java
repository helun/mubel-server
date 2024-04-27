package io.mubel.server.api.grpc.server;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.v1.server.*;
import io.mubel.server.api.grpc.validation.Validators;
import io.mubel.server.eventstore.EventStoreManager;
import io.mubel.server.jobs.BackgroundJobService;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

@GrpcService
public class GrpcMubelServerApi extends MubelServerGrpc.MubelServerImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcMubelServerApi.class);

    private final ProvisionApiService provisionApiService;
    private final EventStoreManager eventStoreManager;
    private final BackgroundJobService jobService;

    public GrpcMubelServerApi(ProvisionApiService provisionApiService, EventStoreManager eventStoreManager, BackgroundJobService jobService) {
        this.provisionApiService = provisionApiService;
        this.eventStoreManager = eventStoreManager;
        this.jobService = jobService;
    }

    @Override
    public void provision(ProvisionEventStoreRequest request, StreamObserver<JobStatus> responseObserver) {
        provisionApiService.provision(request)
                .handle(replyWithJobStatus(responseObserver));
    }

    @Override
    public void drop(DropEventStoreRequest request, StreamObserver<JobStatus> responseObserver) {
        provisionApiService.drop(request)
                .handle(replyWithJobStatus(responseObserver));
    }

    @Override
    public void serverInfo(GetServiceInfoRequest request, StreamObserver<ServiceInfoResponse> responseObserver) {
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

    @Override
    public void eventStoreSummary(GetEventStoreSummaryRequest request, StreamObserver<EventStoreSummary> responseObserver) {
        var validated = Validators.validate(request);
        final var summary = eventStoreManager.getSummary(validated);
        responseObserver.onNext(summary);
        responseObserver.onCompleted();
    }

    @Override
    public void jobStatus(GetJobStatusRequest request, StreamObserver<JobStatus> responseObserver) {
        var validated = Validators.validate(request);
        jobService.awaitJobStatus(validated.getJobId())
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
}
