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