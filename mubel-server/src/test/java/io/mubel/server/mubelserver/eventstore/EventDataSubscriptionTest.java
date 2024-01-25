package io.mubel.server.mubelserver.eventstore;

import io.mubel.api.grpc.EventData;
import io.mubel.api.grpc.SubscribeRequest;
import io.mubel.server.mubelserver.Fixtures;
import io.mubel.server.spi.EventStoreContext;
import io.mubel.server.spi.eventstore.EventStore;
import io.mubel.server.spi.eventstore.LiveEventsService;
import io.mubel.server.spi.eventstore.ReplayService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

class EventDataSubscriptionTest {
    
    TestReplayService replayService = new TestReplayService();

    TestLiveEventsService liveEventsService = new TestLiveEventsService();

    EventStore eventStore;

    EventStoreContext context;

    @BeforeEach
    void setup() {
        context = new EventStoreContext("esid", eventStore, replayService, liveEventsService);
    }

    @Test
    void baseCase() {
        long fromSequenceNo = 99;
        Fixtures.initSequenceNo(fromSequenceNo + 1);
        var request = SubscribeRequest
                .newBuilder()
                .setEsid("esid")
                .setFromSequenceNo(fromSequenceNo)
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
    void shouldRetrySequenceNoOutOfSyncException() {
        long fromSequenceNo = 99;
        Fixtures.initSequenceNo(fromSequenceNo + 1);
        var request = SubscribeRequest
                .newBuilder()
                .setEsid("esid")
                .setFromSequenceNo(fromSequenceNo)
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

    static class TestReplayService implements ReplayService {
        private Queue<List<EventData>> replays = new LinkedList<>();

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

        private Queue<List<EventData>> lives = new LinkedList<>();

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