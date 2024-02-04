package io.mubel.server.test;

import com.google.protobuf.ByteString;
import io.mubel.api.grpc.EventDataInput;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

public class Fixtures {

    private static final Random RANDOM = new Random();

    public static List<EventDataInput> createEvents(int count) {
        var id = uuid();
        return IntStream.range(0, count)
                .mapToObj(version -> event(id, version))
                .toList();
    }

    public static EventDataInput invalidEvent() {
        return eventBuilder()
                .setVersion(-1)
                .build();
    }

    public static EventDataInput event(String streamId, int version) {
        return eventBuilder()
                .setStreamId(streamId)
                .setVersion(version).build();
    }

    public static EventDataInput.Builder eventBuilder() {
        return EventDataInput.newBuilder()
                .setId(uuid())
                .setType("TestEvent")
                .setData(ByteString.copyFrom(randomJson()))
                .setVersion(0);
    }

    public static byte[] randomJson() {
        return """
                {
                   "hash":"%s",
                   "size": %s,
                   "ratio": %s
                   "boolField": %s
                }
                """.formatted(
                uuid(),
                RANDOM.nextInt(),
                RANDOM.nextGaussian(),
                RANDOM.nextBoolean()
        ).getBytes(StandardCharsets.UTF_8);
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }
}
