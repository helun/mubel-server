package io.mubel.server.spi.eventstore;

import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.SubscribeRequest;
import io.mubel.server.spi.DataStream;

public interface ReplayService {

    DataStream<EventData> replay(SubscribeRequest request);

}
