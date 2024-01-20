package io.mubel.server.spi;

import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.SubscribeRequest;

import java.util.concurrent.BlockingQueue;

public interface ReplayService {
    BlockingQueue<EventData> replay(SubscribeRequest request);

}
