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

import io.mubel.provider.jdbc.support.SqlStatements;
import io.mubel.server.spi.eventstore.EventStoreProvisioner;
import io.mubel.server.spi.model.DropEventStoreCommand;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.model.SpiEventStoreDetails;
import org.jdbi.v3.core.Jdbi;

import javax.sql.DataSource;

public class JdbcEventStoreProvisioner implements EventStoreProvisioner {

    private final DataSource dataSource;
    private final SqlStatements provisionStatements;
    private final SqlStatements dropStatements;

    public static void provision(DataSource ds, SqlStatements statements) {
        Jdbi jdbi = Jdbi.create(ds);
        jdbi.useTransaction(h -> statements.forEach(h::execute));
    }

    public static void drop(DataSource ds, SqlStatements statements) {
        Jdbi jdbi = Jdbi.create(ds);

        jdbi.useTransaction(h -> statements.forEach(h::execute));
    }

    public JdbcEventStoreProvisioner(DataSource ds,
                                     SqlStatements provisionStatements,
                                     SqlStatements dropStatements) {
        this.dataSource = ds;
        this.provisionStatements = provisionStatements;
        this.dropStatements = dropStatements;
    }

    @Override
    public SpiEventStoreDetails provision(ProvisionCommand request) {
        JdbcEventStoreProvisioner.provision(dataSource, provisionStatements);
        return null;
    }

    @Override
    public void drop(DropEventStoreCommand command) {
        JdbcEventStoreProvisioner.drop(dataSource, dropStatements);
    }
}
