package io.mubel.server.spi.queue;

import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.UUID;

public interface MessageQueueService {

    void send(SendRequest request);

    void send(BatchSendRequest request);

    Flux<Message> receive(ReceiveRequest request);

    void delete(Collection<UUID> uuids);

    void stop();
}
