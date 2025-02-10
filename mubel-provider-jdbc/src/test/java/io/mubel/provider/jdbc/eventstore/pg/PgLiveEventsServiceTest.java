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
package io.mubel.provider.jdbc.eventstore.pg;

import io.mubel.provider.jdbc.Containers;
import io.mubel.provider.jdbc.eventstore.JdbcEventStore;
import io.mubel.provider.jdbc.eventstore.JdbcEventStoreProvisioner;
import io.mubel.provider.jdbc.support.SqlStatements;
import io.mubel.provider.test.eventstore.LiveEventsServiceTestBase;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executors;

@Testcontainers
class PgLiveEventsServiceTest extends LiveEventsServiceTestBase {

    @Container
    static PostgreSQLContainer<?> container = Containers.postgreSQLContainer();

    static EventStore eventStore;

    static LiveEventsService service;

    @BeforeAll
    static void setup() {

        var dataSource = Containers.dataSource(container);
        String eventStoreName = "test_es";
        JdbcEventStoreProvisioner.provision(dataSource, SqlStatements.of(new PgEventStoreStatements(eventStoreName).ddl()));
        var jdbi = Jdbi.create(dataSource);
        eventStore = new JdbcEventStore(
                jdbi,
                new PgEventStoreStatements(eventStoreName),
                new PgErrorMapper()
        ).init();
        service = new PgLiveEventsService(
                dataSource,
                PgEventStoreStatements.liveChannelName(eventStoreName),
                (JdbcEventStore) eventStore,
                Schedulers.fromExecutor(Executors.newVirtualThreadPerTaskExecutor())
        );
    }

    @AfterAll
    static void tearDown() {
        ((PgLiveEventsService) service).stop();
    }

    @Override
    protected String esid() {
        return "some_esid";
    }

    @Override
    protected EventStore eventStore() {
        return eventStore;
    }

    @Override
    protected LiveEventsService service() {
        return service;
    }
}