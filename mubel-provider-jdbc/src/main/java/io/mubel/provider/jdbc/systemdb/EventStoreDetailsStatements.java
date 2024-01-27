package io.mubel.provider.jdbc.systemdb;

import io.mubel.provider.jdbc.support.RepositoryStatements;

public abstract class EventStoreDetailsStatements implements RepositoryStatements {

    private static final String SELECT = "SELECT esid, provider, type, data_format, state FROM event_store_details ";

    @Override
    public String selectAll() {
        return SELECT + "ORDER BY esid";
    }

    @Override
    public String select() {
        return SELECT + "WHERE esid = ?";
    }

    @Override
    public String delete() {
        return "DELETE FROM event_store_details WHERE esid = ?";
    }

    @Override
    public String exists() {
        return "SELECT EXISTS(SELECT 1 FROM event_store_details WHERE id = ?)";
    }
}
