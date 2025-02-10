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

import io.mubel.api.grpc.v1.events.EventData;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;

public class PollingLiveEventsService extends JdbcLiveEventsService {

    private final int pollingIntervalMs;

    public PollingLiveEventsService(int pollingIntervalMs, JdbcEventStore eventStore, Scheduler scheduler) {
        super(eventStore, scheduler);
        this.pollingIntervalMs = pollingIntervalMs;
    }

    @Override
    protected void run(FluxSink<EventData> emitter) {
        Flux.interval(Duration.ofMillis(pollingIntervalMs))
                .doOnNext(i -> dispatchNewEvents(emitter))
                .blockLast();
    }

    @Override
    protected void onStop() {

    }
}
