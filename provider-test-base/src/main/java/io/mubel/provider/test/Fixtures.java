package io.mubel.provider.test;

import com.google.protobuf.ByteString;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.EventDataInput;

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

public class Fixtures {
    public final static String STREAM_ID_1 = UUID.randomUUID().toString();
    private static long sequenceNo = 0;

    public static void resetSequenceNo() {
        sequenceNo = 0;
    }

    public static EventDataInput eventInput(int version) {
        return eventDataInputBuilder()
                .setRevision(version)
                .build();
    }

    public static EventDataInput eventInput(String streamId, int version) {
        return eventDataInputBuilder()
                .setStreamId(streamId)
                .setRevision(version)
                .build();
    }

    public static EventDataInput.Builder eventDataInputBuilder() {
        return EventDataInput.newBuilder()
                .setId(uuid())
                .setStreamId(STREAM_ID_1)
                .setType("test:event:type")
                .setRevision(0)
                .setData(ByteString.EMPTY);
    }

    public static List<EventDataInput> createEventInputs(int count) {
        var id = uuid();
        return IntStream.range(0, count)
                .mapToObj(version -> eventInput(id, version))
                .toList();
    }

    public static List<EventData> createEvents(int count) {
        var id = uuid();
        return IntStream.range(0, count)
                .mapToObj(version -> event(id, version))
                .toList();
    }

    public static EventData event(String streamId, int revision) {
        return eventDataBuilder()
                .setStreamId(streamId)
                .setRevision(revision).build();
    }

    public static EventData.Builder eventDataBuilder() {
        return EventData.newBuilder()
                .setId(uuid())
                .setStreamId(STREAM_ID_1)
                .setType("test:event:type")
                .setRevision(0)
                .setSequenceNo(sequenceNo++)
                .setCreatedAt(Clock.systemUTC().millis())
                .setData(ByteString.EMPTY);
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    private static String randomEsid() {
        return uuid() + ":esname";
    }
}
