package io.mubel.server.eventstore;

import io.mubel.api.grpc.v1.events.AllSelector;
import io.mubel.api.grpc.v1.events.EventData;
import io.mubel.api.grpc.v1.events.EventSelector;
import io.mubel.api.grpc.v1.events.SubscribeRequest;
import io.mubel.server.Fixtures;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.ExecuteRequestHandler;
import io.mubel.server.spi.eventstore.LiveEventsService;
import io.mubel.server.spi.eventstore.ReplayService;
import io.mubel.server.spi.exceptions.PermissionDeniedException;
import io.mubel.server.spi.groups.LeaderQueries;
import io.mubel.server.spi.queue.MessageQueueService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class EventDataSubscriptionTest {

    TestReplayService replayService = new TestReplayService();

    TestLiveEventsService liveEventsService = new TestLiveEventsService();

    ExecuteRequestHandler executeRequestHandler;

    LeaderQueries leaderQueries = mock(LeaderQueries.class);

    EventStore eventStore;

    EventStoreContext context;

    MessageQueueService messageQueueService;

    @BeforeEach
    void setup() {
        context = new EventStoreContext("esid",
                executeRequestHandler,
                eventStore,
                replayService,
                liveEventsService,
                messageQueueService,
                leaderQueries
        );
    }

    @Test
    void seamless_transition_from_replay_to_live_events() {
        long fromSequenceNo = 99;
        Fixtures.initSequenceNo(fromSequenceNo + 1);
        var request = SubscribeRequest
                .newBuilder()
                .setEsid("esid")
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(fromSequenceNo)
                        )
                )
                .build();

        var replayed = Fixtures.createEvents(3);
        var live = Fixtures.createEvents(3);

        replayService.addReplay(replayed);
        liveEventsService.addLive(live);

        Flux<EventData> subscription = EventDataSubscription.setupSubscription(
                request,
                context
        );

        StepVerifier.create(subscription)
                .expectNextSequence(replayed)
                .expectNextSequence(live)
                .thenCancel()
                .verify();
    }

    @Test
    void replay_restarts_if_a_sequence_number_is_missing() {
        long fromSequenceNo = 99;
        Fixtures.initSequenceNo(fromSequenceNo + 1);
        var request = SubscribeRequest
                .newBuilder()
                .setEsid("esid")
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(fromSequenceNo)
                        )
                )
                .build();

        var firstReplay = Fixtures.createEvents(3);
        var actualLive = Fixtures.createEvents(3);

        replayService.addReplay(firstReplay);
        replayService.addReplay(actualLive);

        liveEventsService.addLive(actualLive.subList(1, 2));

        Flux<EventData> subscription = EventDataSubscription.setupSubscription(
                request,
                context
        );

        StepVerifier.create(subscription)
                .expectNextSequence(firstReplay)
                .expectNextSequence(actualLive)
                .thenCancel()
                .verify();
    }

    @Test
    void subscription_is_rejected_if_supplied_group_token_is_not_leader() {
        var request = SubscribeRequest
                .newBuilder()
                .setEsid("esid")
                .setConsumerGroupToken("not-leader")
                .setSelector(EventSelector.newBuilder()
                        .setAll(AllSelector.newBuilder()
                                .setFromSequenceNo(0)
                        )
                ).build();

        when(leaderQueries.isLeader(anyString())).thenReturn(false);

        Flux<EventData> subscription = EventDataSubscription.setupSubscription(
                request,
                context
        );

        StepVerifier.create(subscription)
                .expectError(PermissionDeniedException.class)
                .verify();
    }

    static class TestReplayService implements ReplayService {
        private final Queue<List<EventData>> replays = new LinkedList<>();

        public void addReplay(List<EventData> replay) {
            replays.add(replay);
        }

        @Override
        public Flux<EventData> replay(SubscribeRequest request) {
            if (replays.isEmpty()) {
                return Flux.empty();
            }
            return Flux.fromIterable(replays.poll());
        }
    }

    static class TestLiveEventsService implements LiveEventsService {

        private final Queue<List<EventData>> lives = new LinkedList<>();

        public void addLive(List<EventData> live) {
            lives.add(live);
        }

        @Override
        public Flux<EventData> liveEvents() {
            if (lives.isEmpty()) {
                return Flux.empty();
            }
            return Flux.fromIterable(lives.poll());
        }
    }
}