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
package io.mubel.provider.inmemory.configuration;

import io.mubel.api.grpc.v1.server.DataFormat;
import io.mubel.provider.inmemory.InMemProviderTestApplication;
import io.mubel.server.spi.Provider;
import io.mubel.server.spi.model.ProvisionCommand;
import io.mubel.server.spi.systemdb.EventStoreDetailsRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = InMemProviderTestApplication.class)
@TestPropertySource("classpath:application-test-properties")
class InMemAutoConfigurationTest {

    @Autowired
    EventStoreDetailsRepository eventStoreDetailsRepository;

    @Autowired
    Provider provider;

    @Test
    void provision() {
        String esid = "test-esid";
        var command = new ProvisionCommand(
                UUID.randomUUID().toString(),
                esid,
                DataFormat.JSON,
                "in-memory"
        );
        provider.provision(command);
        var context = provider.openEventStore(esid);
        assertThat(context).isNotNull();
        assertThat(context.executeRequestHandler()).isNotNull();
        assertThat(context.replayService()).isNotNull();
        assertThat(context.liveEventsService()).isNotNull();
    }

}