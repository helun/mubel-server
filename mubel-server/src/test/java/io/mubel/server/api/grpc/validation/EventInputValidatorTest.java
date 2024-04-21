package io.mubel.server.api.grpc.validation;

import io.mubel.api.grpc.v1.events.EventDataInput;
import io.mubel.server.Fixtures;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventInputValidatorTest {

    @Test
    void non_uuid_id_is_not_valid() {
        var input = Fixtures.eventInput(0)
                .toBuilder()
                .setId("not-uuid")
                .build();
        assertNotValid(input);
    }

    @Test
    void non_uuid_streamId_is_not_valid() {
        var input = Fixtures.eventInput(0)
                .toBuilder()
                .setStreamId("not-uuid")
                .build();
        assertNotValid(input);
    }

    @Test
    void negative_revision_is_not_valid() {
        var input = Fixtures.eventInput(0)
                .toBuilder()
                .setRevision(-1)
                .build();
        assertNotValid(input);
    }

    @Test
    void type_should_be_a_safe_string() {
        var input = Fixtures.eventInput(0)
                .toBuilder()
                .setType("not safe; DROP TABLE users; --")
                .build();
        assertNotValid(input);
    }

    private static void assertNotValid(EventDataInput input) {
        var violations = EventInputValidator.VALIDATOR.validate(input);
        assertThat(violations.isValid()).isFalse();
    }

}