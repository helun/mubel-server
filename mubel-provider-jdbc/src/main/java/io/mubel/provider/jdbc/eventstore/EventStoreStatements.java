package io.mubel.provider.jdbc.eventstore;

public abstract class EventStoreStatements {

    private final String eventStoreName;
    private final String append;
    private final String logRequestSql;
    private final String insertEventType;
    private final String insertStream;
    private final String selectStreamIds;
    private final String getSql;
    private final String getMaxVersionSql;
    private final String pagedReplaySql;
    private final String getAllEventTypes;
    private final String ddl;
    private final String dropSql;

    protected EventStoreStatements(String eventStoreName, String append, String logRequestSql, String insertEventType, String insertStream, String selectStreamIds, String getSql, String getMaxVersionSql, String pagedReplaySql, String getAllEventTypes, String ddl, String dropSql) {
        this.eventStoreName = eventStoreName;
        this.append = append;
        this.logRequestSql = logRequestSql;
        this.insertEventType = insertEventType;
        this.insertStream = insertStream;
        this.selectStreamIds = selectStreamIds;
        this.getSql = getSql;
        this.getMaxVersionSql = getMaxVersionSql;
        this.pagedReplaySql = pagedReplaySql;
        this.getAllEventTypes = getAllEventTypes;
        this.ddl = ddl;
        this.dropSql = dropSql;
    }

    public String append() {
        return append;
    }

    public String logRequestSql() {
        return logRequestSql;
    }

    public String eventStoreName() {
        return eventStoreName;
    }

    public String insertEventType() {
        return insertEventType;
    }

    public String insertStream() {
        return insertStream;
    }

    public String selectStreamIds() {
        return selectStreamIds;
    }

    public String getSql() {
        return getSql;
    }

    public String getMaxVersionSql() {
        return getMaxVersionSql;
    }

    public String pagedReplaySql() {
        return pagedReplaySql;
    }

    public String getAllEventTypes() {
        return getAllEventTypes;
    }

    public int parseSizeLimit(int size) {
        return size == 0 ? 999 : size;
    }

    public String ddl() {
        return ddl;
    }

    public String dropSql() {
        return dropSql;
    }

    public abstract String getSequenceNoSql();

    public abstract String replaySql();

    public abstract String truncate();
}
