package io.mubel.provider.jdbc.eventstore;

import com.google.protobuf.ByteString;
import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.MetaData;
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
                .setVersion(rs.getInt(3))
                .setType(rs.getString(4))
                .setCreatedAt(rs.getLong(5))
                .setData(ByteString.copyFrom(rs.getBytes(6)))
                .setMetaData(metaData)
                .build();
    }
}
