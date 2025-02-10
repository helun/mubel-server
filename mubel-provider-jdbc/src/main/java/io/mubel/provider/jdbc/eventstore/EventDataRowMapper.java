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

import com.google.protobuf.ByteString;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.MetaData;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EventDataRowMapper implements RowMapper<EventData> {

    private final EventData.Builder builder = EventData.newBuilder();

    @Override
    public EventData map(ResultSet rs, StatementContext ctx) throws SQLException {
        var metaDataBytes = rs.getBytes(7);
        MetaData metaData = null;
        if (!rs.wasNull()) {
            try {
                metaData = MetaData.parseFrom(metaDataBytes);
            } catch (Exception e) {
                throw new SQLException("Failed to parse metadata", e);
            }
        }
        return builder
                .setId(rs.getString(1))
                .setStreamId(rs.getString(2))
                .setRevision(rs.getInt(3))
                .setType(rs.getString(4))
                .setCreatedAt(rs.getLong(5))
                .setData(ByteString.copyFrom(rs.getBytes(6)))
                .setMetaData(metaData)
                .setSequenceNo(rs.getLong(8))
                .build();
    }
}
