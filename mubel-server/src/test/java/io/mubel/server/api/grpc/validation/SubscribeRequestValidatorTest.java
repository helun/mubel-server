package io.mubel.server.api.grpc.validation;

import io.mubel.api.grpc.v1.events.AllSelector;
import io.mubel.api.grpc.v1.events.EventSelector;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SubscribeRequestValidatorTest {

    @Test
    void valid_request() {
        var request = SubscribeRequest.newBuilder()
                .setEsid("esid")
                .setTimeout(20)
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(400)
                        )
                ).setMaxEvents(200)
                .build();
        assertValid(request);
    }

    @Test
    void valid_request_required_fields() {
        var request = SubscribeRequest.newBuilder()
                .setEsid("esid")
                .setTimeout(20)
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(400)
                        )
                )
                .build();
        assertValid(request);
    }

    @Test
    void invalid_timeout_does_not_validate() {
        var request = SubscribeRequest.newBuilder()
                .setEsid("esid")
                .setTimeout(21)
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(400)
                        )
                ).setMaxEvents(200)
                .build();
        assertInvalid(request);
    }

    @Test
    void invalid_max_events_does_not_validate() {
        var request = SubscribeRequest.newBuilder()
                .setEsid("esid")
                .setTimeout(20)
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(400)
                        )
                ).setMaxEvents(0)
                .build();
        assertInvalid(request);
    }

    private static void assertInvalid(SubscribeRequest request) {
        var violations = SubscribeRequestValidator.validate(request);
        assertThat(violations.isValid()).isFalse();
    }

    private static void assertValid(SubscribeRequest request) {
        var violations = SubscribeRequestValidator.validate(request);
        assertThat(violations.isValid()).isTrue();
    }

}