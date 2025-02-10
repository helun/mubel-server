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
package io.mubel.provider.jdbc.eventstore;

import java.util.Collection;
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

    public String getMaxRevisionSql() {
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

    public Iterable<?> convertUUIDs(Collection<String> input) {
        return input;
    }

    public abstract String currentRevisionsSql(int paramSize);

}
