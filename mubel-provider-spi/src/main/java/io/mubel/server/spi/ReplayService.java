package io.mubel.server.spi;

import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.SubscribeRequest;

public interface ReplayService {
    
    DataStream<EventData> replay(SubscribeRequest request);

}
