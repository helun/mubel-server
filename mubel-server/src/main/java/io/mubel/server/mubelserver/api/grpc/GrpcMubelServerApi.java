package io.mubel.server.mubelserver.api.grpc;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.*;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

@GrpcService
public class GrpcMubelServerApi extends EventServiceGrpc.EventServiceImplBase {

    @Autowired
    EventStoreApiService eventStoreApiService;

    @Autowired
    SubscribeApiService subscribeApiService;

    @Override
    public void provision(ProvisionEventStoreRequest request, StreamObserver<JobStatus> responseObserver) {
        eventStoreApiService.provision(request, responseObserver);
    }

    @Override
    public void drop(DropEventStoreRequest request, StreamObserver<JobStatus> responseObserver) {
        eventStoreApiService.drop(request, responseObserver);
    }

    @Override
    public void append(AppendRequest request, StreamObserver<AppendAck> responseObserver) {
        eventStoreApiService.append(request, responseObserver);
    }

    @Override
    public void get(GetEventsRequest request, StreamObserver<GetEventsResponse> responseObserver) {
        eventStoreApiService.get(request, responseObserver);
    }

    @Override
    public void serverInfo(GetServiceInfoRequest request, StreamObserver<ServiceInfoResponse> responseObserver) {
        eventStoreApiService.backendInfo(request, responseObserver);
    }

    @Override
    public void subscribe(SubscribeRequest request, StreamObserver<EventData> responseObserver) {
        subscribeApiService.subscribe(request, responseObserver);
    }
}
