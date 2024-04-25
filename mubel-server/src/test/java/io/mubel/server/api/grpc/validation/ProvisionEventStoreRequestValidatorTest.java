package io.mubel.server.api.grpc.validation;

import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.api.grpc.v1.server.ProvisionEventStoreRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProvisionEventStoreRequestValidatorTest {

    @Test
    void valid_request() {
        var request = ProvisionEventStoreRequest.newBuilder()
                .setEsid("esid")
                .setDataFormat(DataFormat.JSON)
                .setWaitForOpen(true)
                .setStorageBackendName("storageBackendName")
                .build();
        assertValid(request);
    }

    @Test
    void invalid_storage_backend_name_does_not_validate() {
        var request = ProvisionEventStoreRequest.newBuilder()
                .setEsid("esid")
                .setDataFormat(DataFormat.JSON)
                .setWaitForOpen(true)
                .setStorageBackendName("foo/bar")
                .build();
        assertInvalid(request);
    }

    void assertValid(ProvisionEventStoreRequest request) {
        var violations = ProvisionEventStoreRequestValidator.VALIDATOR.validate(request);
        assertTrue(violations.isValid());
    }

    void assertInvalid(ProvisionEventStoreRequest request) {
        var violations = ProvisionEventStoreRequestValidator.VALIDATOR.validate(request);
        assertFalse(violations.isValid());
    }

}