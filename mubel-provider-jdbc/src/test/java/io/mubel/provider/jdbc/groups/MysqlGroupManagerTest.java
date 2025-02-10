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
package io.mubel.provider.jdbc.groups;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.support.mysql.MysqlJdbiFactory;
import io.mubel.provider.jdbc.systemdb.SystemDbMigrator;
import io.mubel.provider.jdbc.topic.TestTopic;
import io.mubel.provider.jdbc.topic.Topic;
import io.mubel.provider.test.groups.GroupManagerTestBase;
import io.mubel.server.spi.groups.GroupManager;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Testcontainers
class MysqlGroupManagerTest extends GroupManagerTestBase {

    @Container
    static MySQLContainer<?> container = Containers.mySqlContainer();

    Scheduler scheduler;

    JdbcGroupManager groupManager;

    Topic topic;

    Jdbi jdbi;

    @BeforeAll
    static void setup() {
        var migrator = SystemDbMigrator.migrator(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        migrator.migrate();
    }

    @BeforeEach
    void start() {
        jdbi = MysqlJdbiFactory.create(Containers.dataSource(container));
        scheduler = Schedulers.boundedElastic();
        topic = new TestTopic();
        groupManager = JdbcGroupManager.builder()
                .jdbi(jdbi)
                .topic(topic)
                .heartbeatInterval(heartbeatInterval())
                .clock(clock())
                .scheduler(scheduler)
                .operations(new MySqlGroupManagerOperations())
                .build();
        groupManager.start();
    }

    @AfterEach
    void tearDown() {
        scheduler.dispose();
        jdbi.useHandle(handle -> {
            handle.execute("TRUNCATE group_session");
            handle.execute("TRUNCATE group_leader");
        });
    }

    @Override
    protected GroupManager groupManager() {
        return groupManager;
    }
}
