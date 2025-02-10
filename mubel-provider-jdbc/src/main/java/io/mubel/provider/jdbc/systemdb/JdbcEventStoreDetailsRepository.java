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

import io.mubel.provider.jdbc.support.CrudRepositoryBase;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Update;

public class JdbcEventStoreDetailsRepository extends CrudRepositoryBase<SpiEventStoreDetails> implements EventStoreDetailsRepository {
    
    public JdbcEventStoreDetailsRepository(Jdbi jdbi, EventStoreDetailsStatements statements) {
        super(jdbi, statements, SpiEventStoreDetails.class);
    }

    @Override
    protected Update bind(SpiEventStoreDetails value, Update update) {
        return update.bind(0, value.esid())
                .bind(1, value.provider())
                .bind(2, value.type())
                .bind(3, value.dataFormat())
                .bind(4, value.state());
    }
}
