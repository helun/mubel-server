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
package io.mubel.provider.jdbc.systemdb.pg;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.systemdb.EventStoreDetailsRowMapper;
import io.mubel.provider.jdbc.systemdb.JdbcEventStoreDetailsRepository;
import io.mubel.provider.jdbc.systemdb.SystemDbMigrator;
import io.mubel.provider.test.systemdb.EventStoreDetailsRepositoryTestBase;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class PgEventStoreDetailsRepositoryTest extends EventStoreDetailsRepositoryTestBase {

    @Container
    static PostgreSQLContainer container = Containers.postgreSQLContainer();

    static JdbcEventStoreDetailsRepository repository;

    @BeforeAll
    static void setup() {
        var dataSource = Containers.dataSource(container);
        var migrator = SystemDbMigrator.migrator(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        migrator.migrate();
        var jdbi = Jdbi.create(dataSource);
        jdbi.registerRowMapper(new EventStoreDetailsRowMapper());
        repository = new JdbcEventStoreDetailsRepository(jdbi, new PgEventStoreDetailsStatements());
    }

    @Override
    protected EventStoreDetailsRepository repository() {
        return repository;
    }
}
