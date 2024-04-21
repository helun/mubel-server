package io.mubel.server.api.grpc.validation;

import io.mubel.api.grpc.v1.events.ExecuteRequest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExecuteRequestValidatorTest {

    @Test
    void valid_request() {
        var request = ExecuteRequest.newBuilder()
                .setEsid("esid")
                .build();
        var violations = ExecuteRequestValidator.validate(request);
        assertThat(violations.isValid()).isTrue();
    }

    @Test
    void invalid_esid_does_not_validate() {
        var request = ExecuteRequest.newBuilder()
                .setEsid("")
                .build();
        var violations = ExecuteRequestValidator.validate(request);
        assertThat(violations.isValid()).isFalse();
    }

}