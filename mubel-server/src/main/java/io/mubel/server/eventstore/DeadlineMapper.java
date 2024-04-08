package io.mubel.server.eventstore;

import com.google.protobuf.InvalidProtocolBufferException;
import io.mubel.api.grpc.v1.events.Deadline;
import io.mubel.server.spi.exceptions.BackendException;
import io.mubel.server.spi.queue.Message;

public class DeadlineMapper {

    public static Deadline map(Message message) {
        return parse(message);
    }

    private static Deadline parse(Message message) {
        try {
            return Deadline.parseFrom(message.payload());
        } catch (InvalidProtocolBufferException e) {
            throw new BackendException("Failed to parse ScheduledEvent", e);
        }
    }

}
