package io.mubel.server.mubelserver.api.grpc;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.*;
import io.mubel.server.mubelserver.eventstore.EventStoreManager;
import org.springframework.stereotype.Service;

@Service
public class EventStoreApiService {

    private static final AppendAck APPEND_ACK = AppendAck.newBuilder().build();
    private final EventStoreManager eventStoreManager;

    public EventStoreApiService(EventStoreManager eventStoreManager) {
        this.eventStoreManager = eventStoreManager;
    }

    public void append(AppendRequest request, StreamObserver<AppendAck> responseObserver) {
        eventStoreManager.resolveEventStore(request.getEsid())
                .append(request);
        responseObserver.onNext(APPEND_ACK);
        responseObserver.onCompleted();
    }

    public void provision(ProvisionEventStoreRequest request, StreamObserver<EventStoreDetails> responseObserver) {
        final var details = eventStoreManager.provision(request);
        responseObserver.onNext(EventStoreDetails.newBuilder()
                .setEsid(details.esid())
                .setDataFormat(details.dataFormat())
                .setType(details.type().name())
                .build());
        responseObserver.onCompleted();
    }

    public void drop(DropEventStoreRequest request, StreamObserver<DropEventStoreResponse> responseObserver) {
        var response = eventStoreManager.drop(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
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
                .sorted((o1, o2) -> o1.getName().compareToIgnoreCase(o2.getName()))
                .toList();

        final var details = eventStoreManager.getAllEventStoreDetails();

        responseObserver.onNext(ServiceInfoResponse.newBuilder()
                .addAllEventStore(details)
                .addAllStorageBackend(backends)
                .build());
        responseObserver.onCompleted();
    }
}
