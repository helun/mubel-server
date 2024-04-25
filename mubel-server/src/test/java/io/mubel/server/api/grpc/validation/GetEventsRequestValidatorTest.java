package io.mubel.server.api.grpc.validation;

import io.mubel.api.grpc.v1.events.AllSelector;
import io.mubel.api.grpc.v1.events.EventSelector;
import io.mubel.api.grpc.v1.events.GetEventsRequest;
import io.mubel.api.grpc.v1.events.StreamSelector;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class GetEventsRequestValidatorTest {

    @Test
    void valid_all_request() {
        var request = GetEventsRequest.newBuilder()
                .setEsid("esid")
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(400)
                        )
                        .build())
                .build();
        assertValid(request);
    }

    @Test
    void invalid_esid_does_not_validate() {
        var request = GetEventsRequest.newBuilder()
                .setEsid("")
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(400)
                        )
                        .build())
                .build();
        assertInvalid(request);
    }

    @Test
    void negative_sequence_no_does_not_validate() {
        var request = GetEventsRequest.newBuilder()
                .setEsid("esid")
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(-1)
                        )
                        .build())
                .build();
        assertInvalid(request);
    }

    @Test
    void valid_stream_selector() {
        var request = GetEventsRequest.newBuilder()
                .setEsid("esid")
                .setSelector(EventSelector.newBuilder()
                        .setStream(StreamSelector.newBuilder()
                                .setStreamId(UUID.randomUUID().toString())
                                .setFromRevision(0)
                                .setToRevision(1)
                        )
                )
                .build();
        assertValid(request);
    }

    @Test
    void invalid_streamId_in_stream_selector() {
        var request = GetEventsRequest.newBuilder()
                .setEsid("esid")
                .setSelector(EventSelector.newBuilder()
                        .setStream(StreamSelector.newBuilder()
                                .setStreamId("not an uuid")
                                .setFromRevision(1)
                                .setToRevision(0)
                        )
                )
                .build();
        assertInvalid(request);
    }

    @Test
    void invalid_from_to_revision_in_stream_selector() {
        var request = GetEventsRequest.newBuilder()
                .setEsid("esid")
                .setSelector(EventSelector.newBuilder()
                        .setStream(StreamSelector.newBuilder()
                                .setStreamId(UUID.randomUUID().toString())
                                .setFromRevision(1)
                                .setToRevision(0)
                        )
                )
                .build();
        assertInvalid(request);
    }

    private static void assertInvalid(GetEventsRequest request) {
        var violations = GetEventsRequestValidator.validate(request);
        assertThat(violations.isValid()).isFalse();
    }

    private static void assertValid(GetEventsRequest request) {
        var violations = GetEventsRequestValidator.validate(request);
        assertThat(violations.isValid()).isTrue();
    }

}