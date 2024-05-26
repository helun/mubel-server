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
        return ErrorMessages.eventRevisionConflict(MysqlUtil.decodeUuid(streamId).toString(), Integer.parseInt(version));
    }
}
