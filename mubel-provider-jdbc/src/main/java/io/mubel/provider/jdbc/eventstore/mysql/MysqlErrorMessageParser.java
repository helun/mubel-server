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
package io.mubel.provider.jdbc.eventstore.mysql;

import io.mubel.server.spi.ErrorMessages;

public class MysqlErrorMessageParser {

    public static String parseVersionConflictError(String mysqlErrorDesc) {
        // Duplicate entry '\xD0>;\xE2\xE1WE\x89\x86\xB1\xA5\x00\xE2N\x8E\xC7-1' for key 'myevents.myevents_sid_ver'
        var start = mysqlErrorDesc.indexOf("'") + 1;
        var end = mysqlErrorDesc.indexOf("'", start + 1);
        var key = mysqlErrorDesc.substring(start, end);
        var delimIdx = key.indexOf("-");
        var streamId = key.substring(0, delimIdx);
        var version = key.substring(delimIdx + 1);
        try {
            return ErrorMessages.eventRevisionConflict(MysqlUtil.decodeUuid(streamId).toString(), Integer.parseInt(version));
        } catch (Exception e) {
            return "Failed to parse version conflict error: " + mysqlErrorDesc;
        }

    }
}
