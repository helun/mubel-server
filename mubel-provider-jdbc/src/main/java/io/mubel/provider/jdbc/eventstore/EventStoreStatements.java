package io.mubel.provider.jdbc.eventstore;

import java.util.List;

public abstract class EventStoreStatements {

    private final String eventStoreName;
    private final String append;
    private final String logRequestSql;
    private final String getSql;
    private final String getMaxVersionSql;
    private final String pagedReplaySql;
    private final List<String> ddl;
    private final List<String> dropSql;

    protected EventStoreStatements(String eventStoreName, String append, String logRequestSql, String getSql, String getMaxVersionSql, String pagedReplaySql, List<String> ddl, List<String> dropSql) {
        this.eventStoreName = eventStoreName;
        this.append = append;
        this.logRequestSql = logRequestSql;
        this.getSql = getSql;
        this.getMaxVersionSql = getMaxVersionSql;
        this.pagedReplaySql = pagedReplaySql;
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

    public String getSql() {
        return getSql;
    }

    public String getMaxVersionSql() {
        return getMaxVersionSql;
    }

    public String pagedReplaySql() {
        return pagedReplaySql;
    }

    public int parseSizeLimit(int size) {
        return size == 0 ? 999 : size;
    }

    public List<String> ddl() {
        return ddl;
    }

    public List<String> dropSql() {
        return dropSql;
    }

    public abstract String getSequenceNoSql();

    public abstract String replaySql();

    public abstract List<String> truncate();

    public abstract String summarySql();

    public Object convertUUID(String value) {
        return value;
    }
}
