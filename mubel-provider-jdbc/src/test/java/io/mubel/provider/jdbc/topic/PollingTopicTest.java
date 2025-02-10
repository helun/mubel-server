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
package io.mubel.provider.jdbc.topic;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.support.mysql.MysqlJdbiFactory;
import io.mubel.provider.jdbc.systemdb.SystemDbMigrator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Testcontainers
class PollingTopicTest extends TopicTestBase {

    @Container
    static MySQLContainer<?> container = Containers.mySqlContainer();

    PollingTopic topic;

    Scheduler scheduler;

    @BeforeAll
    static void setup() {
        var migrator = SystemDbMigrator.migrator(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        migrator.migrate();
    }

    @BeforeEach
    void start() {
        var jdbi = MysqlJdbiFactory.create(Containers.dataSource(container));
        scheduler = Schedulers.boundedElastic();
        topic = new PollingTopic(TOPIC_NAME, 200, jdbi, scheduler);
    }

    @AfterEach
    void tearDown() {
        scheduler.dispose();
    }

    @Override
    protected Topic topic() {
        return topic;
    }
}