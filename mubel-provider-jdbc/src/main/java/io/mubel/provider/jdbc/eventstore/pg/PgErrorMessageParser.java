package io.mubel.provider.jdbc.eventstore.pg;

import io.mubel.server.spi.ErrorMessages;

import java.util.regex.Pattern;

public class PgErrorMessageParser {

    private final static Pattern STREAMID_VERSION_EXISTS_PTRN =
            Pattern.compile("Key \\(stream_id, revision\\)=\\((.+), (\\d+)\\) already exists\\.");

    public static String parseRevisionConflictError(String errorDetail) {
        // Key (stream_id, version)=(8e9368ed-88cf-452f-9076-8a2a7b276914, 1) already exists.
        var m = STREAMID_VERSION_EXISTS_PTRN.matcher(errorDetail);
        if (m.find()) {
            var streamId = m.group(1);
            var version = m.group(2);
            return ErrorMessages.eventRevisionConflict(streamId, Integer.parseInt(version));
        } else {
            return errorDetail;
        }
    }

}
