package io.mubel.provider.jdbc.systemdb.mysql;

import io.mubel.provider.jdbc.systemdb.EventStoreDetailsStatements;

public class MysqlEventStoreDetailsStatements extends EventStoreDetailsStatements {

    @Override
    public String upsert() {
        return """
                INSERT INTO event_store_details (esid, provider, type, data_format, state)
                VALUES (?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    provider = VALUES(provider),
                    type = VALUES(type),
                    data_format = VALUES(data_format),
                    state = VALUES(state)
                """;
    }
}
