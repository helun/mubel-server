package io.mubel.provider.jdbc.eventstore.pg;

import io.mubel.server.spi.ErrorMessages;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PgErrorMessageParserTest {


    @Test
    void parseVersionConflictError() {
        var message = "ERROR: duplicate key value violates unique constraint \"events_sid_ver\"\n" +
                "  Detail: Key (stream_id, revision)=(a8fad578-d79e-4446-8c63-be4396700b77, 1) already exists.";

        var parsed = PgErrorMessageParser.parseRevisionConflictError(message);
        assertThat(parsed).isEqualTo(ErrorMessages.eventRevisionConflict("a8fad578-d79e-4446-8c63-be4396700b77", 1));
    }

}