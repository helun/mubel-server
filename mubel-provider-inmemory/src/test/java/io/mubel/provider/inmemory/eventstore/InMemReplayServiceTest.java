/*
 * mubel-provider-inmemory - Multi Backend Event Log
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
package io.mubel.provider.inmemory.eventstore;

import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.provider.test.eventstore.ReplayServiceTestBase;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.ReplayService;
import io.mubel.server.spi.model.ProvisionCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.util.Set;

class InMemReplayServiceTest extends ReplayServiceTestBase {

    public static final String TEST_ESID = "test-esid";
    static EventStore eventStore;

    static InMemEventStores eventStores = new InMemEventStores(Set.of("in-memory"));

    static ReplayService replayService = new InMemReplayService(eventStores);

    @BeforeAll
    static void start() {
        var details = eventStores.provision(new ProvisionCommand(
                "jobid",
                TEST_ESID,
                DataFormat.JSON,
                "in-memory"
        ));
        eventStore = eventStores.create(details);
    }

    @AfterEach
    void tearDown() {
        eventStore.truncate();
    }

    @Override
    protected String esid() {
        return TEST_ESID;
    }

    @Override
    protected EventStore eventStore() {
        return eventStore;
    }

    @Override
    protected ReplayService service() {
        return replayService;
    }
}