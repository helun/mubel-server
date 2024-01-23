package io.mubel.server.mubelserver.api.grpc;

import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.SubscribeRequest;
import org.springframework.stereotype.Service;

@Service
public class SubscribeApiService {


    public void subscribe(SubscribeRequest request, StreamObserver<EventData> responseObserver) {
        
    }
}
