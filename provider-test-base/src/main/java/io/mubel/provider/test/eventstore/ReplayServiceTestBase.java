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

import io.mubel.api.grpc.v1.events.*;
import io.mubel.provider.test.Fixtures;
import io.mubel.provider.test.TestSubscriber;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.ReplayService;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class ReplayServiceTestBase {

    protected abstract String esid();

    protected abstract EventStore eventStore();

    protected abstract ReplayService service();

    @RepeatedTest(9)
    void replay_are_streamed_in_order() {
        int count = 1024;
        setupEvents(count);
        var request = SubscribeRequest
                .newBuilder()
                .setEsid(esid())
                .build();
        TestSubscriber<EventData> testSubscriber = new TestSubscriber<>(service().replay(request));

        testSubscriber
                .awaitDone()
                .assertComplete()
                .assertValueCount(count);
    }

    @Test
    void replaying_from_sequence_no_returns_events_starting_from_specified_sequence_exclusive() {
        int count = 10;
        var events = setupEvents(count);
        var middle = events.get(4);
        var request = SubscribeRequest
                .newBuilder()
                .setEsid(esid())
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(middle.getSequenceNo())
                        )
                )
                .build();
        TestSubscriber<EventData> testSubscriber = new TestSubscriber<>(service().replay(request));
        testSubscriber.awaitCount(5)
                .awaitDone()
                .assertComplete()
                .assertNoErrors();
        assertThat(testSubscriber.values()).hasSize(5);
        var values = testSubscriber.values();
        assertThat(values)
                .containsExactlyElementsOf(events.subList(5, 10));
    }

    @Test
    void replaying_an_empty_store_ends_without_events_or_errors() {
        var request = SubscribeRequest
                .newBuilder()
                .setEsid(esid())
                .build();
        TestSubscriber<EventData> testSubscriber = new TestSubscriber<>(service().replay(request));
        testSubscriber.awaitDone();
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
    }

    private List<EventData> setupEvents(int count) {
        var events = Fixtures.createEventInputs(count);
        var request = AppendOperation.newBuilder()
                .addAllEvent(events)
                .build();
        eventStore().append(request);
        return eventStore().get(GetEventsRequest.newBuilder()
                .setEsid(esid())
                .setSize(count)
                .build()
        ).getEventList();
    }

}
