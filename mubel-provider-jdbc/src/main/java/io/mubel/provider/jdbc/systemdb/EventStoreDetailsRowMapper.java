package io.mubel.provider.jdbc.systemdb;

import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.server.spi.eventstore.EventStoreState;
import io.mubel.server.spi.model.BackendType;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import org.jdbi.v3.core.mapper.RowViewMapper;
import org.jdbi.v3.core.result.RowView;

import java.sql.SQLException;

public class EventStoreDetailsRowMapper implements RowViewMapper<SpiEventStoreDetails> {

    @Override
    public SpiEventStoreDetails map(RowView rowView) throws SQLException {
        return new SpiEventStoreDetails(
                rowView.getColumn(1, String.class),
                rowView.getColumn(2, String.class),
                rowView.getColumn(3, BackendType.class),
                rowView.getColumn(4, DataFormat.class),
                rowView.getColumn(5, EventStoreState.class)
        );
    }
}
