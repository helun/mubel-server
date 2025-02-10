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
package io.mubel.provider.jdbc.eventstore.mysql;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.eventstore.JdbcEventStoreProvisioner;
import io.mubel.provider.jdbc.queue.mysql.MysqlMessageQueueStatements;
import io.mubel.provider.jdbc.support.SqlStatements;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class MysqlProvisionTest {

    @Container
    static JdbcDatabaseContainer<?> container = Containers.mySqlContainer();

    @Test
    void drop_removes_all_db_objects() {
        var dataSource = Containers.dataSource(container);
        String eventStoreName = "test_es";
        var statements = new MysqlEventStoreStatements(eventStoreName);
        var queueStatements = new MysqlMessageQueueStatements();
        JdbcEventStoreProvisioner.provision(
                dataSource,
                SqlStatements.of(
                        statements.ddl(),
                        queueStatements.ddl()
                )
        );
        var jdbi = Jdbi.create(dataSource);
        var tableNames = getTableNames(jdbi);
        assertThat(tableNames)
                .hasSize(4);
        
        JdbcEventStoreProvisioner.drop(
                dataSource,
                SqlStatements.of(
                        statements.dropSql(),
                        queueStatements.dropSql()
                )
        );
        assertThat(getTableNames(jdbi)).isEmpty();
    }

    private static List<String> getTableNames(Jdbi jdbi) {
        return jdbi.withHandle(h ->
                h.queryMetadata(dbmd -> dbmd.getTables(container.getDatabaseName(), null, null, null))
                        .map(view -> view.getColumn("table_name", String.class))
                        .list()
        );
    }
}
