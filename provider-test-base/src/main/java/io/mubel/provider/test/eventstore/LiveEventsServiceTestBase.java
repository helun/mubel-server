/*
 * provider-test-base - Multi Backend Event Log
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
package io.mubel.provider.test.eventstore;

import io.mubel.api.grpc.v1.events.AppendOperation;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.GetEventsRequest;
import io.mubel.provider.test.Fixtures;
import io.mubel.provider.test.TestSubscriber;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class LiveEventsServiceTestBase {

    protected abstract String esid();

    protected abstract EventStore eventStore();

    protected abstract LiveEventsService service();

    @Test
    void new_events_are_published_in_append_order() {
        setupEvents(5);
        TestSubscriber<EventData> testSubscriber = new TestSubscriber<>(service().liveEvents());
        await().during(Duration.ofSeconds(1))
                .untilAsserted(testSubscriber::assertNoValues);

        setupEvents(5);
        await().failFast(testSubscriber::assertNoErrors)
                .until(() -> testSubscriber.values().size() == 5);
        assertThat(testSubscriber.values()).hasSize(5)
                .map(EventData::getSequenceNo)
                .containsExactly(6L, 7L, 8L, 9L, 10L);
    }

    private void setupEvents(int count) {
        var events = Fixtures.createEventInputs(count);
        var request = AppendOperation.newBuilder()
                .addAllEvent(events)
                .build();
        eventStore().append(request);
        await().until(() -> eventStore().get(GetEventsRequest.newBuilder()
                .setEsid(esid())
                .setSize(count)
                .build()
        ).getEventCount() == count);
    }
}
