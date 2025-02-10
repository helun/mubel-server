/*
 * mubel-provider-jdbc - mubel-provider-jdbc
 * Copyright © 2025 Henrik Barratt-Due (henrikbd@hey.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
