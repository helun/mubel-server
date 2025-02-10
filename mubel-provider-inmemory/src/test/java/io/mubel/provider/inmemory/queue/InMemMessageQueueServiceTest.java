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
package io.mubel.provider.inmemory.queue;

import io.mubel.provider.test.queue.MessageQueueServiceTestBase;
import io.mubel.server.spi.queue.MessageQueueService;
import io.mubel.server.spi.queue.QueueConfiguration;
import io.mubel.server.spi.queue.QueueConfigurations;
import io.mubel.server.spi.support.IdGenerator;
import org.junit.jupiter.api.AfterEach;

import java.time.Duration;
import java.util.List;

class InMemMessageQueueServiceTest extends MessageQueueServiceTestBase {

    InMemMessageQueueService service = new InMemMessageQueueService(new IdGenerator() {
    }, new QueueConfigurations(List.of(
            new QueueConfiguration(QUEUE_NAME, VISIBILITY_TIMEOUT, Duration.ofMillis(500))
    )));

    @AfterEach
    void tearDown() {
        service.reset();
    }

    @Override
    protected MessageQueueService service() {
        return service;
    }


}