package io.mubel.provider.test.eventstore;

import io.mubel.api.grpc.v1.events.*;
import io.mubel.provider.test.Fixtures;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.exceptions.EventRevisionConflictException;
import org.junit.jupiter.api.*;

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
            var request = AppendOperation.newBuilder()
                    .addAllEvent(events)
                    .build();
            eventStore.append(request);
            var response = eventStore.get(
                    GetEventsRequest.newBuilder()
                            .setSelector(EventSelector.newBuilder()
                                    .setStream(StreamSelector.newBuilder()
                                            .setStreamId(streamId))
                            )
                            .setEsid(esid())
                            .setSize(100)
                            .build()
            );
            assertThat(response.getEventList()).hasSize(25);
            assertEvents(streamId, response, 0);
        }

        @Disabled("TODO: implement duplicate request handling in higher level")
        @Test
        void duplicate_requests_are_ignored() {
            var events = Fixtures.createEventInputs(25);
            var streamId = events.getFirst().getStreamId();
            var request = AppendOperation.newBuilder()
                    //.setRequestId(UUID.randomUUID().toString())
                    .addAllEvent(events)
                    .build();
            eventStore.append(request);
            eventStore.append(request);
            var response = eventStore.get(
                    GetEventsRequest.newBuilder()
                            .setSelector(EventSelector.newBuilder()
                                    .setStream(StreamSelector.newBuilder()
                                            .setStreamId(streamId))
                            )
                            .setEsid(esid())
                            .setSize(100)
                            .build()
            );
            assertThat(response.getEventList()).hasSize(25);
        }

        @Test
        void appending_an_event_with_conflicting_revision_throws_EventRevisionConflictException() {
            var events = Fixtures.createEventInputs(2);
            var request = AppendOperation.newBuilder()
                    .addAllEvent(events)
                    .build();
            eventStore.append(request);
            var e2 = events.get(1);
            var conflictRequest = AppendOperation.newBuilder()
                    .addEvent(Fixtures.eventInput(e2.getStreamId(), e2.getRevision()))
                    .build();
            assertThatThrownBy(() -> eventStore.append(conflictRequest))
                    .isInstanceOfSatisfying(EventRevisionConflictException.class,
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
        void streamId_and_from_revision_returns_events_starting_from_specified_revision() {
            var count = 10;
            var streamId = appendEvents(count);
            int fromRevision = 4;
            var response = eventStore.get(
                    GetEventsRequest.newBuilder()
                            .setEsid(esid())
                            .setSelector(EventSelector.newBuilder()
                                    .setStream(StreamSelector.newBuilder()
                                            .setStreamId(streamId)
                                            .setFromRevision(fromRevision)
                                    )
                            )
                            .setSize(100)
                            .build()
            );
            assertThat(response.getEventList())
                    .as("from revision > 0 should limit results from head")
                    .hasSize(6)
                    .map(EventData::getRevision)
                    .first()
                    .as("first event should have revision %s", fromRevision)
                    .isEqualTo(fromRevision);
        }

        @Test
        void max_revision_returns_events_up_to_specified_revision() {
            var count = 10;
            var streamId = appendEvents(count);
            int fromRevision = 4;
            int toRevision = 8;
            var response = eventStore.get(
                    GetEventsRequest.newBuilder()
                            .setEsid(esid())
                            .setSelector(EventSelector.newBuilder()
                                    .setStream(StreamSelector.newBuilder()
                                            .setStreamId(streamId)
                                            .setFromRevision(fromRevision)
                                            .setToRevision(toRevision)
                                    )
                            )
                            .setSize(100)
                            .build()
            );
            assertThat(response.getEventList())
                    .as("from revision > 0 should limit results from head")
                    .hasSize(5)
                    .map(EventData::getRevision)
                    .as("revision should be (%s, %s)", fromRevision, toRevision)
                    .containsExactly(4, 5, 6, 7, 8);
        }

        @Test
        void paging_events() {
            var events = Fixtures.createEventInputs(25);
            var streamId = events.getFirst().getStreamId();
            var request = AppendOperation.newBuilder()
                    .addAllEvent(events)
                    .build();
            eventStore.append(request);
            var pageSize = 10;
            var page0 = GetEventsRequest.newBuilder()
                    .setSelector(EventSelector.newBuilder()
                            .setStream(StreamSelector.newBuilder()
                                    .setStreamId(streamId)
                            )
                    )
                    .setEsid(esid())
                    .setSize(pageSize)
                    .build();
            var p0Response = eventStore.get(page0);
            assertThat(p0Response.getEventList()).hasSize(pageSize);
            assertEvents(streamId, p0Response, 0);

            var p1Response = eventStore.get(GetEventsRequest.newBuilder(page0)
                    .setSelector(EventSelector.newBuilder()
                            .setStream(StreamSelector.newBuilder()
                                    .setStreamId(streamId)
                                    .setFromRevision(10)
                            )
                    )
                    .build());
            assertThat(p1Response.getEventList()).hasSize(pageSize);
            assertEvents(streamId, p1Response, 10);

            var p2Response = eventStore.get(GetEventsRequest.newBuilder(page0)
                    .setSelector(EventSelector.newBuilder()
                            .setStream(StreamSelector.newBuilder()
                                    .setStreamId(streamId)
                                    .setFromRevision(20)
                            )
                    )
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
        var request = AppendOperation.newBuilder()
                .addAllEvent(events)
                .build();
        eventStore.append(request);
        return events.getFirst().getStreamId();
    }

    private void assertEvents(final String streamId, final GetEventsResponse response, final int fromRevision) {
        var expectedRevision = new AtomicInteger(fromRevision);
        assertThat(response.getEventList()).allSatisfy(e -> {
            assertThat(e.getStreamId()).isEqualTo(streamId);
            assertThat(e.getRevision()).isEqualTo(expectedRevision.getAndIncrement());
        });
    }

    abstract protected String esid();
}
