package io.mubel.provider.jdbc.systemdb.pg;

import io.mubel.provider.jdbc.systemdb.EventStoreDetailsStatements;

public class PgEventStoreDetailsStatements extends EventStoreDetailsStatements {
    
    @Override
    public String upsert() {
        return """
                INSERT INTO event_store_details (esid, provider, type, data_format, state)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (esid) DO UPDATE SET
                    provider = EXCLUDED.provider,
                    type = EXCLUDED.type,
                    data_format = EXCLUDED.data_format,
                    state = EXCLUDED.state
                """;
    }

}
