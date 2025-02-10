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
        return "SELECT EXISTS(SELECT 1 FROM event_store_details WHERE esid = ?)";
    }
}
