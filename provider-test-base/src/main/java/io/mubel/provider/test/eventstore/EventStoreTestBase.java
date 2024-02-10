package io.mubel.provider.test.eventstore;

import io.mubel.api.grpc.AppendRequest;
import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.GetEventsRequest;
import io.mubel.api.grpc.GetEventsResponse;
import io.mubel.provider.test.Fixtures;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.exceptions.EventVersionConflictException;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class EventStoreTestBase {

    static protected EventStore eventStore;

    @Nested
    class Append {
        @Test
        void append_persists_all_events_in_order() {
            var events = Fixtures.createEventInputs(25);
            var streamId = events.getFirst().getStreamId();
            var request = AppendRequest.newBuilder()
                    .setEsid(esid())
                    .addAllEvent(events)
                    .build();
            eventStore.append(request);
            var response = eventStore.get(
                    GetEventsRequest.newBuilder()
                            .setStreamId(streamId)
                            .setEsid(esid())
                            .setSize(100)
                            .build()
            );
            assertThat(response.getEventList()).hasSize(25);
            assertEvents(streamId, response, 0);
        }

        @Test
        void duplicate_requests_are_ignored() {
            var events = Fixtures.createEventInputs(25);
            var streamId = events.getFirst().getStreamId();
            var request = AppendRequest.newBuilder()
                    .setEsid(esid())
                    .setRequestId(UUID.randomUUID().toString())
                    .addAllEvent(events)
                    .build();
            eventStore.append(request);
            eventStore.append(request);
            var response = eventStore.get(
                    GetEventsRequest.newBuilder()
                            .setStreamId(streamId)
                            .setEsid(esid())
                            .setSize(100)
                            .build()
            );
            assertThat(response.getEventList()).hasSize(25);
        }

        @Test
        void appending_an_event_with_conflicting_version_throws_EventVersionConflictException() {
            var events = Fixtures.createEventInputs(2);
            var request = AppendRequest.newBuilder()
                    .setEsid(esid())
                    .addAllEvent(events)
                    .build();
            eventStore.append(request);
            var e2 = events.get(1);
            var conflictRequest = AppendRequest.newBuilder()
                    .setEsid(esid())
                    .addEvent(Fixtures.eventInput(e2.getStreamId(), e2.getVersion()))
                    .build();
            assertThatThrownBy(() -> eventStore.append(conflictRequest))
                    .isInstanceOfSatisfying(EventVersionConflictException.class,
                            err -> assertThat(err.getMessage())
                                    .startsWith("event: streamId:")
                                    .endsWith("already exists")
                    );
        }
    }

    @Nested
    class Get_request {
        @Test
        void without_streamId_returns_events_in_global_order() {
            var count = 10;
            appendEvents(count);

            var response = eventStore.get(
                    GetEventsRequest.newBuilder()
                            .setEsid(esid())
                            .setSize(100)
                            .build()
            );
            assertThat(response.getEventList()).hasSize(count);
        }

        @Test
        void streamId_and_from_version_returns_events_starting_from_specified_version() {
            var count = 10;
            var streamId = appendEvents(count);
            int fromVersion = 4;
            var response = eventStore.get(
                    GetEventsRequest.newBuilder()
                            .setEsid(esid())
                            .setStreamId(streamId)
                            .setFromVersion(fromVersion)
                            .setSize(100)
                            .build()
            );
            assertThat(response.getEventList())
                    .as("from version > 0 should limit results from head")
                    .hasSize(6)
                    .map(EventData::getVersion)
                    .first()
                    .as("first event should have version %s", fromVersion)
                    .isEqualTo(fromVersion);
        }

        @Test
        void max_version_returns_events_up_to_specified_version() {
            var count = 10;
            var streamId = appendEvents(count);
            int fromVersion = 4;
            int toVersion = 8;
            var response = eventStore.get(
                    GetEventsRequest.newBuilder()
                            .setEsid(esid())
                            .setStreamId(streamId)
                            .setFromVersion(fromVersion)
                            .setToVersion(toVersion)
                            .setSize(100)
                            .build()
            );
            assertThat(response.getEventList())
                    .as("from version > 0 should limit results from head")
                    .hasSize(5)
                    .map(EventData::getVersion)
                    .as("versions should be (%s, %s)", fromVersion, toVersion)
                    .containsExactly(4, 5, 6, 7, 8);
        }

        @Test
        void paging_events() {
            var events = Fixtures.createEventInputs(25);
            var streamId = events.getFirst().getStreamId();
            var request = AppendRequest.newBuilder()
                    .setEsid(esid())
                    .addAllEvent(events)
                    .build();
            eventStore.append(request);
            var pageSize = 10;
            var page0 = GetEventsRequest.newBuilder()
                    .setStreamId(streamId)
                    .setEsid(esid())
                    .setSize(pageSize)
                    .build();
            var p0Response = eventStore.get(page0);
            assertThat(p0Response.getEventList()).hasSize(pageSize);
            assertEvents(streamId, p0Response, 0);

            var p1Response = eventStore.get(GetEventsRequest.newBuilder(page0)
                    .setFromVersion(10)
                    .build());
            assertThat(p1Response.getEventList()).hasSize(pageSize);
            assertEvents(streamId, p1Response, 10);

            var p2Response = eventStore.get(GetEventsRequest.newBuilder(page0)
                    .setFromVersion(20)
                    .build());
            assertThat(p2Response.getEventList()).hasSize(5);
            assertEvents(streamId, p2Response, 20);
        }
    }

    @Nested
    class Summary {

        @Test
        void returns_event_count_and_stream_count() {
            var count = 10;
            appendEvents(count);
            var summary = eventStore.summary();
            assertThat(summary.getEventCount()).isEqualTo(count);
            assertThat(summary.getStreamCount()).isEqualTo(1);
        }

        @Test
        void returns_zero_for_empty_event_store() {
            var summary = eventStore.summary();
            assertThat(summary.getEventCount()).isZero();
            assertThat(summary.getStreamCount()).isZero();
        }

    }

    private String appendEvents(int count) {
        var events = Fixtures.createEventInputs(count);
        var request = AppendRequest.newBuilder()
                .setEsid(esid())
                .addAllEvent(events)
                .build();
        eventStore.append(request);
        return events.getFirst().getStreamId();
    }

    private void assertEvents(final String streamId, final GetEventsResponse response, final int fromVersion) {
        var expectedVersion = new AtomicInteger(fromVersion);
        assertThat(response.getEventList()).allSatisfy(e -> {
            assertThat(e.getStreamId()).isEqualTo(streamId);
            assertThat(e.getVersion()).isEqualTo(expectedVersion.getAndIncrement());
        });
    }

    abstract protected String esid();
}
