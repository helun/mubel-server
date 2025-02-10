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
