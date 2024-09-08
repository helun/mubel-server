package io.mubel.server.api.grpc.events;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.mubel.api.grpc.v1.events.*;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcService
public class GrpcMubelEventsApi extends MubelEventsServiceGrpc.MubelEventsServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcMubelEventsApi.class);

    private final ExecuteApiService executeService;
    private final EventApiService eventApiService;
    private final SubscribeApiService subscribeApiService;

    public GrpcMubelEventsApi(
            ExecuteApiService executeService,
            EventApiService eventApiService,
            SubscribeApiService subscribeApiService
    ) {
        this.executeService = executeService;
        this.eventApiService = eventApiService;
        this.subscribeApiService = subscribeApiService;
    }

    @Override
    public void execute(ExecuteRequest request, StreamObserver<Empty> responseObserver) {
        LOG.debug("received execute request: {}, operation count: {}", request.getRequestId(), request.getOperationCount());
        long start = System.currentTimeMillis();
        executeService.execute(request)
                .handle((ignored, err) -> {
                    if (err != null) {
                        responseObserver.onError(err);
                        LOG.error("failed to execute request: {}", request.getRequestId(), err);
                    } else {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                        long duration = System.currentTimeMillis() - start;
                        LOG.debug("executed request: {} in {} ms", request.getRequestId(), duration);
                    }
                    return null;
                });
    }

    @Override
    public void getEvents(GetEventsRequest request, StreamObserver<GetEventsResponse> responseObserver) {
        var result = eventApiService.get(request);
        responseObserver.onNext(result);
        responseObserver.onCompleted();
    }

    @Override
    public void getEventStream(GetEventsRequest request, StreamObserver<EventData> responseObserver) {
        eventApiService.getEventStream(request, responseObserver);
    }

    @Override
    public void subscribe(SubscribeRequest request, StreamObserver<EventData> responseObserver) {
        subscribeApiService.subscribe(request, responseObserver);
    }

    @Override
    public void subcribeToDeadlines(DeadlineSubscribeRequest request, StreamObserver<Deadline> responseObserver) {
        subscribeApiService.subcribeToDeadlines(request, responseObserver);
    }

}
