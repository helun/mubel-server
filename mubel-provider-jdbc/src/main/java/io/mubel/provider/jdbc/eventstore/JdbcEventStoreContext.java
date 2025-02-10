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

import io.mubel.provider.jdbc.queue.JdbcMessageQueueService;
import io.mubel.server.spi.eventstore.ReplayService;

public record JdbcEventStoreContext(
        JdbcEventStore eventStore,
        JdbcEventStoreProvisioner provisioner,
        ReplayService replayService,
        JdbcLiveEventsService liveEventsService,
        JdbcMessageQueueService messageQueueService
) {
    public void close() {
        liveEventsService.stop();
        messageQueueService.stop();
    }
}
