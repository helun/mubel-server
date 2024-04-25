package io.mubel.server.api.grpc.validation;

import io.mubel.api.grpc.v1.server.DropEventStoreRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DropEventStoreRequestValidatorTest {

    @Test
    void valid_request() {
        var request = DropEventStoreRequest.newBuilder()
                .setEsid("esid")
                .build();
        assertValid(request);
    }

    @Test
    void invalid_esid_does_not_validate() {
        var request = DropEventStoreRequest.newBuilder()
                .setEsid("foo/bar; DROP TABLE event_store;")
                .build();
        assertInvalid(request);
    }

    private void assertValid(DropEventStoreRequest request) {
        var violations = DropEventStoreRequestValidator.VALIDATOR.validate(request);
        assertTrue(violations.isValid());
    }

    private void assertInvalid(DropEventStoreRequest request) {
        var violations = DropEventStoreRequestValidator.VALIDATOR.validate(request);
        assertFalse(violations.isValid());
    }

}