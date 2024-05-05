package io.mubel.server.api.grpc.validation;

import io.mubel.api.grpc.v1.events.AppendOperation;
import io.mubel.api.grpc.v1.events.ExecuteRequest;
import io.mubel.api.grpc.v1.events.Operation;
import io.mubel.server.Fixtures;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExecuteRequestValidatorTest {

    @Test
    void valid_request() {
        var request = ExecuteRequest.newBuilder()
                .setEsid("esid")
                .addOperation(Operation.newBuilder()
                        .setAppend(AppendOperation.newBuilder()
                                .addEvent(Fixtures.eventInput(0)))
                )
                .build();
        assertValid(request);
    }

    @Test
    void invalid_esid_does_not_validate() {
        var request = ExecuteRequest.newBuilder()
                .setEsid("")
                .build();
        assertInvalid(request);
    }

    @Test
    void invalid_event_does_not_validate() {
        var request = ExecuteRequest.newBuilder()
                .setEsid("esid")
                .addOperation(Operation.newBuilder()
                        .setAppend(AppendOperation.newBuilder()
                                .addEvent(Fixtures.eventInput(0)
                                        .toBuilder()
                                        .setId("not-uuid")
                                        .build()))
                )
                .build();
        assertInvalid(request);
    }

    private static void assertInvalid(ExecuteRequest request) {
        var violations = ExecuteRequestValidator.validate(request);
        assertThat(violations.isValid()).isFalse();
    }

    private static void assertValid(ExecuteRequest request) {
        var violations = ExecuteRequestValidator.validate(request);
        assertThat(violations.isValid()).isTrue();
    }

}